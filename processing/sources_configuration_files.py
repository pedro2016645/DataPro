"""
Module to read yaml files from mapped sources from local data packages
"""
import pkgutil
import yaml
import logging
import os

logger = logging.getLogger(__name__)


def read_yaml(src_tag: str, package: str) -> dict:
    """
    Function to read yaml_files inside data_packages
    :param src_tag:
    :param package:
    :return:
    """
    content = {}
    try:
        assert type(src_tag) == str, "The given src_tag must be a string"
        assert type(package) == str, "The given package must be a string"

        try:
            # get data
            raw_data = pkgutil.get_data(package, '{0}.yaml'.format(src_tag))

            content = yaml.load(raw_data.decode('ANSI'), Loader=yaml.FullLoader)

        except ValueError as e:
            logger.error(e)

    except AssertionError as e:
        logger.error(e)

    return content

def read_yaml_by_path(path: str, encoding: str) -> dict:
    """
    Function to read yaml_files from path
    :param path:
    :param encoding:
    :return:
    """
    content = {}
    isfile = os.path.isfile(path)
    valid_encoding = ['ANSI', 'UTF-8']

    logger.info("Yaml by path")
    try:
        assert type(path) == str, "The path must be a string"
        assert type(encoding) == str, "The given encoding must be a string"
        assert encoding in valid_encoding, "The given encoding is not valid"
        assert isfile == True, "The given path is not valid"

        try:
            # get data
            raw_data = open(path,encoding=encoding)

            content = yaml.load(raw_data, Loader=yaml.FullLoader)
            logger.info("YAML read.")

        except ValueError as e:
            logger.error(e)
            logger.info("YAML error.")
            raise e

    except AssertionError as e:
        logger.error(e)
        raise e

    return content


def read_sql(file_name: str, package: str):
    """

    :param file_name:
    :param package:
    :return:
    """

    content = ""
    try:
        assert type(file_name) == str, "The given file_name must be a string"
        assert type(package) == str, "The given package must be a string"

        try:
            # get data
            content = pkgutil.get_data(package, '{0}'.format(file_name)).decode('UTF-8')

        except ValueError as e:
            logger.error(e)

    except AssertionError as e:
        logger.error(e)

    return content


def check_cols_data_types(data_types_dict: dict, cols_list: list) -> None:
    """
    Check for each column if it is identified with more than one datatype or with no datatype
    :param cols_list:
    :param data_types_dict:
    :return:
    """
    try:
        assert type(data_types_dict) == dict, "The given src_setup must be a dict"
        assert type(cols_list) == list, "The given cols_list must be a dict"

        # List with column types
        list_type_cols = ['n_client_cols', 'str_cols', 'int_cols', 'nif_cols', 'float_cols',
                          'contact_cols', 'date_cols']

        # Verify if the src_setup[data_types] have all datatypes
        missing_type_cols = [x for x in list_type_cols if x not in data_types_dict.keys()]
        assert len(missing_type_cols) == 0, \
            "The column(s) {} are missing from configurations data_types".format(','.join(missing_type_cols))

        # Make a list with all the columns with identified type
        cols_present_in_data_types = []
        for d_type in list_type_cols:
            if d_type in data_types_dict and data_types_dict[d_type] is not None:
                if d_type != 'date_cols':
                    cols_present_in_data_types.extend(
                        [x for x in data_types_dict[d_type].split(',')]
                    )
                else:
                    if data_types_dict[d_type]['cols'] is not None:
                        cols_present_in_data_types.extend(
                            [x for x in data_types_dict[d_type]['cols'].split(',')]
                        )

        for col in cols_list:
            # Create a warning that are columns not identified with any data type
            if col not in cols_present_in_data_types:
                logger.warning('The column {} have no data_type identified'.format(col))
            # Create a warning that are columns with mores than one type identified
            if cols_present_in_data_types.count(col) > 1:
                logger.warning("The column {} have more than one data_type identified".format(col))

    except AssertionError as e:
        logger.error(e)
