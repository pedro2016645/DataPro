import os
import logging
import pandas as pd
from datetime import datetime

from ..preparation.sources import Standardization
from ..preparation.sources import MaskData
from ..preparation.time_handlers import compare_refresh_rate
from ..processing.sources_configuration_files import read_yaml, check_cols_data_types
from ..processing.data_quality import FileCorrections
from ..processing.upload import ReadFiles
from ..processing.download import SaveData
from ..processing.workspace import DirectoryOperations

logger = logging.getLogger(__name__)

DATE_FORMAT_SOURCE = "%Y%m%d_%H%M%S"

FUNCTION_MAPPING = {
    'date_cols': Standardization.dates,
    'float_cols': Standardization.floats,
    'nif_cols': Standardization.nifs,
    'str_cols': Standardization.strings,
    'int_cols': Standardization.ints,
    'n_client_cols': Standardization.ints,
    'mask_data': '',
    'names': Standardization.columns_names
}

COLS_TAGS = {
    'data_types': {
        'level1': ['names',
                   'n_client_cols',
                   'str_cols',
                   'int_cols',
                   'nif_cols',
                   'float_cols',
                   'contact_cols',
                   'macro_tags_names',
                   'micro_tags_names'],
        'level2': {
            'date_cols': 'cols'
        }
    }
}


def fix_yaml_columns(source_setup: dict, multilevel: bool = False, yaml_structure=None) -> dict:
    """
    Remove extra space and \n on columns list
    Only works for 2 level of keys on a yaml
    :param multilevel:
    :param yaml_structure:
    :type yaml_structure:
    :param source_setup:
    :type source_setup:
    :return:
    :rtype:
    """
    if yaml_structure is None:
        yaml_structure = COLS_TAGS

    # For each datatypes see if the datatype is or not date_cols
    for level_0 in yaml_structure.keys():
        if 'level1' in yaml_structure[level_0]:
            # if not date_cols correct the string with columns
            for key in yaml_structure[level_0]['level1']:
                if key in source_setup[level_0] and source_setup[level_0][key] is not None:
                    if multilevel and type(source_setup[level_0][key]) == dict:
                        # in multilevel each date_type have a dictionary with the micro_tag whose value is a
                        # string with the columns
                        all_cols_list = []
                        for micro_tag in source_setup[level_0][key].keys():
                            cols_list = source_setup[level_0][key][micro_tag]
                            if cols_list is not None:
                                assert type(cols_list) == str, \
                                    "The value of the key {} in micro_tag {} must be a string.".format(key, micro_tag)
                                # Replace the enters in the string => .replace("\n", "")
                                # Split by ',' in order to have the list of columns =>.split(',')
                                # For each column remove the spaces in the beginning and the ending of the
                                # string => .strip
                                cols_list = [c.strip() for c in cols_list.replace("\n", "").split(',')]
                                # In multilevel case: join into the beginning of the string the name of the micro_tag
                                all_cols_list.extend(['{}_{}'.format(micro_tag, c) for c in cols_list])
                        # Turn the list into a string
                        cols_list = ','.join(all_cols_list)

                    else:
                        cols_list = source_setup[level_0][key]
                        assert type(cols_list) == str, \
                            "The value of the key {} must be a string.".format(key)
                        cols_list = [c.strip() for c in cols_list.replace("\n", "").split(',')]
                        cols_list = ','.join(cols_list)
                    source_setup[level_0][key] = cols_list

        if 'level2' in yaml_structure[level_0]:
            for key in yaml_structure[level_0]['level2'].keys():
                if key in source_setup[level_0] and source_setup[level_0][key] is not None:
                    value = yaml_structure[level_0]['level2'][key]
                    if (multilevel and (type(source_setup[level_0][key]) == dict) and
                            (value not in source_setup[level_0][key])):
                        all_cols_list = []
                        for micro_tag in source_setup[level_0][key]:
                            assert value in source_setup[level_0][key][micro_tag].keys(), \
                                "The {} must be present in the keys of {} to the micro_tag {}" \
                                    .format(value, key, micro_tag)
                            cols_list = source_setup[level_0][key][micro_tag][value]
                            if cols_list is not None:
                                assert type(cols_list) == str, \
                                    "The value of the {} in the key {} and micro_tag {} must be a string." \
                                        .format(value, key, micro_tag)
                                # Replace the enters in the string => .replace("\n", "")
                                # Split by ',' in order to have the list of columns =>.split(',')
                                # For each column remove the spaces in the beginning and the ending of the
                                # string => .strip
                                cols_list = [c.strip() for c in cols_list.replace("\n", "").split(',')]
                                cols_in_level2_keys = [x for x in source_setup[level_0][key][micro_tag].keys()
                                                       if x in cols_list]
                                if len(cols_in_level2_keys) > 0:
                                    source_setup[level_0][key] = {
                                        '{}_{}'.format(micro_tag, k)
                                        if k != value
                                        else k: v
                                        for k, v in source_setup[level_0][key][micro_tag].items()}
                                # In multilevel case: join into the beginning of the string the name of the micro_tag
                                all_cols_list.extend(['{}_{}'.format(micro_tag, c) for c in cols_list])
                        # Turn the list into a string
                        cols_list = ','.join(all_cols_list)
                    else:
                        assert value in source_setup[level_0][key].keys(), \
                            "The {} must be present in the keys of {}." \
                                .format(value, key)
                        cols_list = source_setup[level_0][key][value]
                        if cols_list is not None:
                            assert type(cols_list) == str, \
                                "The value of the {} in the key {} must be a string.".format(value, key)
                            cols_list = [c.strip() for c in cols_list.replace("\n", "").split(',')]
                            cols_list = ','.join(cols_list)
                    source_setup[level_0][key][value] = cols_list

    return source_setup


def replace_data_types_tags(transform_params: dict, df: pd.DataFrame):
    """
    Function to deal with tags present in transform_params dictionary:
    Available replace_tags:
    - #all-cols-starts-with#: replace 'colname_#all-cols-starts-with#' by all the columns in the dataframe that
    startswith 'colname'
    :param transform_params:
    :param df:
    :return:
    """
    # Strip columns names
    df.columns = [col.strip() for col in df.columns.tolist()]

    # list with data_types that we need to check if they have replace_tags
    list_type_cols = ['names', 'n_client_cols', 'str_cols', 'int_cols', 'nif_cols', 'float_cols',
                      'contact_cols', 'date_cols']

    # Set replace tags: '#all-cols-starts-with#'
    replace_tags = ['#all-cols-starts-with#']

    for data_type in list_type_cols:
        if data_type in transform_params and transform_params[data_type] is not None:
            if data_type == 'date_cols':
                # get the current content of data_type
                data_type_content = transform_params[data_type].copy()
                # get the columns of the date_cols data_type list
                columns = data_type_content['cols'].split(',')

                for rep_tag in replace_tags:
                    # Check if the replace_tag is in the cols
                    cols_with_rep_tag = [col for col in columns if rep_tag in col]
                    if len(cols_with_rep_tag) > 0:
                        logger.info('Start replacing tag {} for data_type {}.'.format(rep_tag, data_type))
                        # verify if is not a general format but is a format by column, in that case we need to change
                        # the keys in the dictionary
                        date_format = [fmt for fmt in ['mix', 'global_format', 'other_formats']
                                       if fmt in data_type_content.keys()]

                        # get the not changing columns
                        cols_without_rep_tag = [col for col in columns if col not in cols_with_rep_tag]
                        # for each column that have the replacement tag
                        # replace that column by all columns in the dataframe that are coherent with the replacement tag
                        for col_tag in cols_with_rep_tag:
                            col_tag = col_tag.replace('_{}'.format(rep_tag), '')
                            if rep_tag == '#all-cols-starts-with#':
                                # get cols that start with the col_tag
                                find_cols = [col for col in df.columns.tolist() if col.startswith(col_tag)]
                            else:
                                find_cols = []
                            # add the find cols to the cols_without_rep_tag to add this new information
                            cols_without_rep_tag.extend(find_cols)
                            # check the dictionary with date format info
                            if len(date_format) == 0:
                                # if columns names in the dictionary key
                                # replace them by the respective columns with the format date defined to the column
                                # reference to have a replace tag
                                if rep_tag == '#all-cols-starts-with#':
                                    data_type_content = {
                                        col if k.startswith(col_tag) else k: v
                                        for k, v in data_type_content.items()
                                        for col in find_cols
                                    }
                        columns = cols_without_rep_tag
                    data_type_content['cols'] = ','.join(columns)
                transform_params[data_type] = data_type_content
            else:
                # get the data_type columns list
                columns = transform_params[data_type].split(',')
                for rep_tag in replace_tags:
                    # Check if the replace_tag is in the cols
                    cols_with_rep_tag = [col for col in columns if rep_tag in col]
                    if len(cols_with_rep_tag) > 0:
                        logger.info('Start replacing tag {} for data_type {}.'.format(rep_tag, data_type))

                        # get the not changing columns
                        cols_without_rep_tag = [col for col in columns if col not in cols_with_rep_tag]
                        # for each column that have the replacement tag
                        # replace that column by all columns in the dataframe that are coherent with the replacement tag
                        for col_tag in cols_with_rep_tag:
                            col_tag = col_tag.replace('_{}'.format(rep_tag), '')
                            if rep_tag == '#all-cols-starts-with#':
                                # get cols that start with the col_tag
                                find_cols = [col for col in df.columns.tolist() if col.startswith(col_tag)]
                            else:
                                find_cols = []
                            # add the find cols to the cols_without_rep_tag to add this new information
                            cols_without_rep_tag.extend(find_cols)
                        columns = cols_without_rep_tag
                transform_params[data_type] = ','.join(columns)

    return transform_params


def transform(transform_params: dict, tag: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to deal with transform settings
    :param df:
    :type df:
    :param tag:
    :type tag:
    :type transform_params: dict
    """
    if transform_params[tag] is not None and tag != 'names':
        logger.info("Star normalization of {}".format(tag))
        if tag == 'date_cols':
            dictionary = transform_params[tag]
            df = FUNCTION_MAPPING[tag](df, dictionary, transform_params['optional'])
        else:
            columns = transform_params[tag].split(',')
            df = FUNCTION_MAPPING[tag](df, columns, transform_params['optional'])

    elif tag == 'names':
        if 'standardize_cols_names' not in transform_params.keys():
            if_needed = True
        else:
            assert type(transform_params['standardize_cols_names']) == bool, \
                "The standardize_cols_names parameter in data_types must be a boolean"
            if_needed = transform_params['standardize_cols_names']

        if tag not in transform_params or transform_params[tag] is None:
            logger.info('Start normalization of cols {}'.format(tag))
            # if you don't want this transformation if_needed = False

            columns = df.columns.tolist()
            df = FUNCTION_MAPPING[tag](df, columns, if_needed)

        elif tag in transform_params:
            logger.info('Start normalization of cols {}'.format(tag))
            columns = transform_params[tag].split(',')
            df = FUNCTION_MAPPING[tag](df, columns, if_needed)

    return df


def build_query_params(source_setup: dict, date_obj: datetime, src_tag: str, date_part: str = "",
                       test: bool = False) -> dict:
    """
    :param test:
    :param source_setup:
    :param date_obj:
    :param src_tag:
    :param date_part:
    :return:
    """

    try:
        assert type(source_setup) == dict, "The variable source_setup must be a dictionary"
        assert type(date_obj) == datetime, "The variable date_obj must be a datetime type"
        assert type(src_tag) == str, "The src_tag must be a string"

        # Check if the query needs to change a params
        if 'query_manipulation' in source_setup.keys():
            query_params = source_setup['query_manipulation']

            for key in query_params.keys():
                # Add time reference if needs one
                if 'time' in query_params[key].keys():
                    query_params[key]['time']['date_obj'] = date_obj

                if 'time_groups' in query_params[key].keys():
                    if 'date_part' in query_params[key]['time_groups'].keys():
                        query_params[key]['time_groups']['date_part'] = date_part

                # if need a auxiliary source is need we have to save a temp file
                if 'insert_into' in query_params[key].keys():
                    for temp_file_tag in query_params[key]['insert_into']:
                        temp_file_params = query_params[key]['insert_into'][temp_file_tag]
                        # Create temp files
                        if test:
                            parent_folder = os.path.join(
                                os.path.dirname(os.path.abspath('__file__')),
                                'tests//sources'
                            )
                        else:
                            parent_folder = "."
                        possible_path = os.path.join(parent_folder, 'temp_aux_sources')
                        if not os.path.exists(possible_path):
                            os.mkdir(possible_path)

                        possible_path = os.path.join(possible_path, src_tag)
                        if not os.path.exists(possible_path):
                            os.mkdir(possible_path)

                        verify_source = [k for k in ['clean_source', 'prep_source']
                                         if k in temp_file_params.keys()]

                        assert len(verify_source) == 1, \
                            "The 'insert_into' for {} tag must have a key named 'clean_source' or 'prep_source'" \
                                .format(temp_file_tag)
                        if 'clean_source' in temp_file_params.keys():
                            # Read last file of clean_sources
                            aux_source_package = 'shyness.params'
                            source_type = 'clean_source'
                            src_setup = 'processed_source'
                            if 'product' in temp_file_params.keys():
                                aux_source_package = '{}.{}'.format(aux_source_package,
                                                                    temp_file_params['product'])
                        else:
                            # Read last file of prep_sources
                            aux_source_package = 'shad.params'
                            source_type = 'prep_source'
                            src_setup = 'prepared_source'
                            if 'use_case' in temp_file_params.keys():
                                if 'src_type' not in temp_file_params.keys():
                                    temp_file_params['src_type'] = 'prep_data'
                                aux_source_package = '{}.{}.{}'.format(aux_source_package,
                                                                       temp_file_params['use_case'],
                                                                       temp_file_params['src_type'])

                        logger.info('Loading auxiliary source for temp_file_tag {}'.format(temp_file_tag))

                        aux_source_setup = read_yaml(temp_file_params[source_type],
                                                     aux_source_package)

                        aux_source_settings = aux_source_setup[src_setup]['save_parameters']

                        if 'product' in temp_file_params.keys():
                            aux_source_file_path = DirectoryOperations.select_recent_file(
                                os.path.join(aux_source_settings['path'],
                                             temp_file_params['product'],
                                             aux_source_settings['tag']),
                                aux_source_settings['tag'])
                        else:
                            aux_source_file_path = DirectoryOperations.select_recent_file(
                                os.path.join(aux_source_settings['path'],
                                             temp_file_params['use_case'],
                                             temp_file_params['src_type'],
                                             aux_source_settings['tag']),
                                aux_source_settings['tag'])

                        aux_source = ReadFiles.data_file(aux_source_file_path, encoding='utf-8-sig', decimal='.')

                        # Selected needed columns
                        aux_source = aux_source[temp_file_params[
                            'cols_needed'].split(",")].drop_duplicates()

                        # Save as local file
                        logger.info('Saving auxiliary source for temp_file_tag {}'.format(temp_file_tag))
                        aux_source.to_csv(os.path.join(possible_path, "{}.csv".format(temp_file_tag)),
                                          index=False, encoding='UTF-8-SIG', sep=',')
                        query_params[key]['insert_into'][temp_file_tag]['temp_file'] = os.path.abspath(
                            os.path.join(possible_path,
                                         "{}.csv".format(temp_file_tag)))
        else:
            query_params = {}
        return query_params
    except AssertionError as e:
        logger.error(e)
        return {}


def app(src_tag: str, product: str = "master", src_time: str = "", user: str = '', password: str = '', env: str = 'dev',
        file_path: str = "", test: bool = False, data_folder_tests: str = r".\..\shyness\data\tests",
        date_part: str = "") -> bool:
    """
    Function to process a source
    :param file_path:
    :param data_folder_tests:
    :param test:
    :param src_tag:
    :param src_time:
    :param user:
    :param password:
    :param product:
    :param env:
    :param date_part:
    :return:
    """
    successful = False
    try:
        assert type(src_tag) == str, "The given src_tag must be a string"
        assert type(src_time) == str, "The given src_time must be a string"
        assert len(src_tag) > 0, "The given src_tag cannot be not empty"

        try:
            # Check str
            if len(src_time) > 0:
                # Transform to date
                try:
                    date_obj = datetime.strptime(src_time, DATE_FORMAT_SOURCE)
                    date_tag = src_time
                except ValueError as e:
                    logger.error(e)
                    raise ValueError("The given str_time must be on the format: {0}".format(DATE_FORMAT_SOURCE))

            else:
                date_obj = datetime.now()
                date_tag = datetime.now().strftime(DATE_FORMAT_SOURCE)

            # Reading file
            logger.info('Selecting source yaml {}'.format(src_tag))

            # Name packages
            package_name = 'shyness.params'
            package_name = '{}.{}'.format(package_name, product)

            source_setup = read_yaml(src_tag, package_name)
            if ('multilevel' in source_setup['raw_source']['load_parameters']) and \
                    (source_setup['raw_source']['load_parameters']['multilevel']):
                multilevel = True
            else:
                multilevel = False

            source_setup = fix_yaml_columns(source_setup, multilevel)

            # Check if there is a latency date
            if 'refresh_rate' in source_setup['conf_file'].keys():
                refresh_rate_rfr = source_setup['conf_file']['refresh_rate'].split(',')
                time_reference = refresh_rate_rfr[0]
                rate = int(refresh_rate_rfr[1])

            else:
                time_reference = None
                rate = 0

            if 'save_type' in source_setup['processed_source'].keys():
                save_type = source_setup['processed_source']['save_type']
            else:
                save_type = 'file'
            to_save_parameters = source_setup['processed_source']['save_parameters']

            logger.info("Checking if there is an update version")

            if test:
                to_save_parameters['path'] = data_folder_tests
            DirectoryOperations.check_dir(to_save_parameters['path'], product)
            DirectoryOperations.check_dir(os.path.join(to_save_parameters['path'], product), to_save_parameters['tag'])
            # Check which type of source is updated
            recent_file = DirectoryOperations.select_recent_file(os.path.join(to_save_parameters['path'],
                                                                              product,
                                                                              to_save_parameters['tag']),
                                                                 to_save_parameters['tag'])
            not_processed = True
            file_date = recent_file.split('.')[0].replace(os.path.join(to_save_parameters['path'],
                                                                       product,
                                                                       to_save_parameters['tag'],
                                                                       to_save_parameters['tag']) + '_', '')
            if len(recent_file) > 0 and (len(src_time) == 0 and len(file_path) == 0):
                logger.info("Last Update: {} ".format(file_date))
                file_date = datetime.strptime(file_date, DATE_FORMAT_SOURCE)
                not_processed = compare_refresh_rate(file_date, date_obj, time_reference, rate)
            elif len(src_time) != 0 or len(file_path) == 0:
                not_processed = True
            if not_processed:
                logger.warning('No updated version of this source, loading source')
                load_parameters = source_setup['raw_source']['load_parameters']
                load_process = source_setup['raw_source']['type']['specifics']
                transform_params = source_setup['data_types']

                if 'quality' in source_setup['raw_source']['type'].keys():
                    quality_params = source_setup['raw_source']['type']['quality']
                    header = source_setup['data_types']['names'].replace("\n ", "")
                    correction_mng = FileCorrections(quality_params, load_parameters, header)
                    df = correction_mng.save_to_df()

                elif load_process == 'file':
                    if len(file_path) > 0 and os.path.exists(file_path):
                        recent_file_raw = file_path

                    elif len(file_path) == 0:
                        if len(src_time) == 0:
                            recent_file_raw = DirectoryOperations.select_recent_file(load_parameters['path'],
                                                                                     load_parameters['file_name'])
                        else:
                            recent_file_raw = DirectoryOperations.select_close_file_by_date(
                                load_parameters['path'],
                                load_parameters['file_name'],
                                lim_date=src_time,
                                date_format=DATE_FORMAT_SOURCE,
                                structure='{0}_{0}_{1}'.format('{}', load_parameters['file_type']))

                    else:
                        raise AssertionError("{} does not exist".format(file_path))
                    if 'change_col_names' in load_parameters.keys() and load_parameters['change_col_names'] == 'yes':
                        change_col_names = transform_params['names'].split(',')
                    else:
                        change_col_names = None

                    if load_parameters['file_type'] == 'csv':

                        df = ReadFiles.data_file(recent_file_raw, load_parameters['header_row'],
                                                 load_parameters['delimiter'],
                                                 load_parameters['encoding'], load_parameters['special_char'],
                                                 load_parameters['decimal'],
                                                 change_name_init_cols=change_col_names)
                        df = ReadFiles.remove_unnamed(df)

                    elif (load_parameters['file_type'] == 'xlsx') or (load_parameters['file_type'] == 'xls'):
                        if multilevel:
                            # multilevel = True
                            df = ReadFiles.excel(path=recent_file_raw,
                                                 sheet_number=load_parameters['sheet_number'],
                                                 header_row=load_parameters['header_row'],
                                                 multilevel=multilevel,
                                                 macro_tags=transform_params['macro_tags_names'].split(','),
                                                 micro_tags=transform_params['micro_tags_names'].split(','),
                                                 duplicated_macro_tag=transform_params['duplicated_macro_tag'],
                                                 change_name_init_cols=change_col_names
                                                 )
                        else:
                            df = ReadFiles.excel(path=recent_file_raw,
                                                 sheet_number=load_parameters['sheet_number'],
                                                 header_row=load_parameters['header_row'],
                                                 change_name_init_cols=change_col_names,
                                                 decimal=load_parameters['decimal'])
                    else:
                        df = ReadFiles.excel(recent_file_raw, load_parameters['sheet_number'],
                                             load_parameters['header_row'],
                                             change_name_init_cols=change_col_names, decimal=load_parameters['decimal'])

                    df = ReadFiles.remove_unnamed(df)

                else:
                    # Reading data sources from PDA
                    assert user != '', 'Please provide a valid user'
                    assert password != '', "Please provide a valid password"

                    query_params = build_query_params(source_setup, date_obj, src_tag, date_part, test)
                    logger.info("Start setup to run query")
                    df = ReadFiles.table(load_parameters['query_file'],
                                         transform_params['names'].split(','),
                                         product, user, password, env, **query_params)

                if 'optional' not in transform_params:
                    transform_params['optional'] = 0

                # Start standardization
                transform_params = replace_data_types_tags(transform_params, df)
                check_cols_data_types(transform_params, df.columns.tolist())
                if test:
                    if 'tests_params' in source_setup.keys():
                        tests_params = source_setup['tests_params']

                        if ('standardize_cols_names' in tests_params.keys()
                                and tests_params['standardize_cols_names'] is not None):
                            transform_params['standardize_cols_names'] = tests_params['standardize_cols_names']

                for tag in FUNCTION_MAPPING.keys():
                    if tag == 'mask_data':
                        if 'mask_data' in source_setup.keys():
                            mask_parameters = source_setup['mask_data']

                            df = MaskData.cols(df, **mask_parameters)
                    else:
                        df = transform(transform_params, tag, df)

                # Saving clean source
                # TODO: CHECK
                if save_type == 'file':
                    logger.info('Saving file in {}'.format(to_save_parameters['path']))
                    SaveData.write_file(df,
                                        path=to_save_parameters['path'],
                                        product=product,
                                        tag=to_save_parameters['tag'],
                                        date_tag=date_tag,
                                        file_type=to_save_parameters['file_type'],
                                        delimiter=to_save_parameters['delimiter'],
                                        encoding=to_save_parameters['encoding'],
                                        date_part=date_part)

                logger.info("Source updated")
                successful = True

            else:
                logger.info('No need to process this source, there is already an updated version on the folder')

        except ValueError as e:
            logger.error(e)
    except AssertionError as e:
        logger.error(e)

    return successful
