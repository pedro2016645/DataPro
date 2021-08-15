import datetime
import logging
import os
import uuid
import json
import pandas as pd
import re
from .time_handlers import select_validate_format

logger = logging.getLogger(__name__)

DATE_FORMAT_SOURCE = "%Y%m%d_%H%M%S"


class Standardization:

    @classmethod
    def optional_col(cls, optional: int, cols_to_consider: list, cols_source: list) -> list:
        """
        Function that deals with
        :param cols_source:
        :param optional:
        :param cols_to_consider:
        :return:
        """
        # May need more corrections
        if optional == 1:
            cols = []
            for col in cols_to_consider:
                if col in cols_source:
                    cols.append(col)
                else:
                    logger.warning("The col {} is not present on the source".format(col))
            cols_to_consider = [x for x in cols_to_consider if x in cols_source]

        return cols_to_consider

    # TODO: Add a new function on string that handles spaces and names on a different way

    @classmethod
    def strings(cls, df: pd.DataFrame, cols_to_consider: list, optional=0) -> pd.DataFrame:
        """
        This function standardizes strings in the chosen columns of the given dataframe according to some criteria:
        -it fills nan's to empty strings;
        -converts the whole column to string;
        -deletes spaces;
        -deletes strings that are alusive to nan's and none's
        -deletes characters that are not letters, digits or underscores.
        :param optional:
        :param df: Receives a DataFrame
        :param cols_to_consider: Receives the list of columns to change in the specified DataFrame
        :return: DataFrame with the standardized strings
        """

        # Requires the variables to be respectively a DataFrame and a list
        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."
        assert type(cols_to_consider) == list, "The col_to_consider parameter is not a list."

        cols_to_consider = cls.optional_col(optional, cols_to_consider, list(df.columns))

        # For each chosen column from the DataFrame, standardize the strings according to the function description
        for col in cols_to_consider:
            df.loc[:, col] = df[col].fillna("")
            df.loc[:, col] = df[col].astype(str)
            df.loc[:, col] = df[col].str.replace(' ', '')
            df.loc[:, col] = df[col].str.lower()
            df.loc[:, col] = df[col].str.replace('nan', '')
            df.loc[:, col] = df[col].str.replace('none', '')
            df.loc[:, col] = df[col].str.replace(r'[\W]', '')

        return df

    @classmethod
    def dates(cls, df: pd.DataFrame, date_params: dict, optional=0) -> pd.DataFrame:
        """
        This function standardizes dates in the chosen columns of the given dataframe according to some criteria:
        - it fills nan's to empty strings;
        - converts the columns to strings, in order to treat dates as strings before converting them to datetime;
        - if there are less than 4 characters in a certain cell, delete it;
        - convert the strings to datetime including hours, minutes and seconds, in order to choose the most recent
        record,
        if needed.
        :param optional:
        :param date_params: Receives the list of columns to change in the specified DataFrame
        :param df: Receives a DataFrame
        :return: DataFrame with the standardized dates
        """

        # Requires the variables to be respectively a DataFrame and a list
        assert type(df) == pd.DataFrame, "The df parameter is not a DataFrame."
        assert type(date_params) == dict, "The date_params parameter is not a dict."
        # For each chosen column from the DataFrame, standardize the dates according to the function description
        # select cols
        cols_to_consider = date_params['cols'].split(",")
        cols_to_consider = cls.optional_col(optional, cols_to_consider, list(df.columns))

        for col in cols_to_consider:
            logger.info('start normalization for {}'.format(col))
            df.loc[:, col] = df[col].fillna("")
            df.loc[:, col] = df[col].apply(lambda value: str(value))
            df.loc[:, col] = df[col].apply(lambda value: '' if len(value) < 4 else value)
            # Check date format
            if col in date_params.keys():
                format_to_use = date_params[col]
            elif 'other_formats' in date_params.keys():
                format_to_use = date_params['other_formats']
            elif 'mix' in date_params.keys():
                format_to_use = date_params['mix'].split(",")
            else:
                format_to_use = date_params['global_format']

            if format_to_use[-1] == ".":
                df.loc[:, col] = df[col].apply(
                    lambda value:
                    datetime.datetime.strptime(str(value).split(".")[0], format_to_use[:-1]).strftime('%Y%m%d_%H%M%S')
                    if str(value) != 'NaT' and str(value) != '' else value)
            elif format_to_use[-1] == '0':
                df.loc[:, col] = df[col].apply(
                    lambda value:
                    datetime.datetime.strptime(str(value).split(" ")[0], format_to_use[:-1]).strftime('%Y%m%d_%H%M%S')
                    if str(value) != 'NaT' and str(value) != '' else value)
            elif 'mix' in date_params.keys():
                df.loc[:, col] = df[col].apply(
                    lambda value:
                    datetime.datetime.strptime(value, select_validate_format(value,
                                                                             format_to_use)).strftime('%Y%m%d_%H%M%S')
                    if str(value) != 'NaT' and str(value) != '' else value)
            else:
                df.loc[:, col] = df[col].apply(
                    lambda value:
                    datetime.datetime.strptime(str(value), format_to_use).strftime('%Y%m%d_%H%M%S')
                    if str(value) != 'NaT' and str(value) != '' else value)

        return df

    @classmethod
    def floats(cls, df: pd.DataFrame, specific_cols: list, optional=1, decimal=None) -> pd.DataFrame:
        """
        This function standardizes floats in the chosen columns of the given DataFrame according to some criteria:
        - it fills nan's to empty strings;
        - converts the columns to strings, in order to treat floats as strings before converting them to floats;
        - delete the string if it has at least a letter;
        - delete every character that is not a digit, a comma or a period;
        - replace commas for periods;
        - only maintain the last period;
        - in case there is a value without a period, add it with two zeros in front of it;
        - in case there is only one period remaining (without any other character), the string is deleted;
        - in case there is nothing to the left of a period, a zero is added;
        - in case there is nothing to the right of a period, a zero is added;
        - convert the strings to floats.
        :param optional:
        :param df:
        :param specific_cols: Receives the list of columns to change in the specified DataFrame
        :param decimal:
        """
        specific_cols = cls.optional_col(optional, specific_cols, list(df.columns))
        assert type(specific_cols) == list, "The specific_cols variable must be a list"
        list_exclude = [x for x in specific_cols if x not in df.columns.values.tolist()]

        assert len(list_exclude) == 0, "The specific_cols variable: {} must be in the cols_to_consider variable".format(
            list_exclude
        )
        # For each chosen column from the DataFrame, standardize the floats according to the function description
        # May need more corrections

        for col in specific_cols:
            df.loc[:, col] = df[col].fillna('-1')
            df.loc[:, col] = df[col].astype(str)
            df.loc[:, col] = df[col].apply(lambda value: "-1" if value == "." else value)
            df.loc[:, col] = df[col].apply(lambda value: float(str(value).replace("", "-1")) if len(
                str(value)) == 0 else value)
            if any(df[col].str.count(r"\.") > 1):
                df.loc[:, col] = df[col].str.replace('.', '')
            elif any(df[col].str.count(r"\.") == 1) and decimal == ",":
                df.loc[:, col] = df[col].apply(lambda value: value.replace(".", "") if value.find(".") > 0 and len(
                    value.split('.')[1]) > 2 else value)
            elif any(df[col].str.count(',') > 1):
                df.loc[:, col] = df[col].str.replace(',', '')
            elif any(df[col].str.count(',') == 1) and decimal == ".":
                df.loc[:, col] = df[col].apply(lambda value: value.replace(",", "") if value.find(",") > 0 and len(
                    value.split(',')[1]) > 2 else value)
            if any(df[col].str.count("%") > 0):
                df.loc[:, col] = df[col].str.replace('%', '')
            df.loc[:, col] = df[col].apply(lambda value: float(str(value).replace(",", ".")) if len(
                str(value).split(",")) == 2 and str(value).find(".") < 0 else value)
            df.loc[:, col] = df[col].apply(
                lambda value: float(str(value)) if len(str(value).split(".")) == 2 and str(value).find(',') < 0
                else value)
            df.loc[:, col] = df[col].apply(lambda value: float(str(value).replace(".", "").replace(",", ".")
                                                               ) if len(str(value).split(".")) > 2 else value)
            df.loc[:, col] = df[col].apply(lambda value:
                                           float("{}.{}".format("".join(str(value).replace(".", "").split(",")[:-1]),
                                                                str(value).split(",")[-1]))
                                           if len(str(value).split(",")) > 1 and len(
                                               str(value).split(",")[-1]) == 2 and str(value).find(".") > 0
                                           else value)
            df.loc[:, col] = df[col].apply(lambda value: -1 if str(value).find(":") > 0 else value)
            df.loc[:, col] = df[col].astype(float)
            df.loc[:, col] = df[col].replace(-1, 0)
        return df

    @classmethod
    def ints(cls, df: pd.DataFrame, specific_cols: list, optional=0) -> pd.DataFrame:
        """
        This function standardizes ints in the chosen columns of the given DataFrame according to some criteria:
        - it fills nan's to empty strings;
        - converts the columns to strings, in order to treat ints as strings before converting them to ints;
        - replace commas for periods;
        - delete white spaces;
        - only maintain the last period;
        - split the string by periods in case there is one, in order to keep the int part of the string (no
        approximation);
        - check the int for non-digits: in case the boolean returns false, create an empty string;
        - convert the strings to ints.
        :param optional:
        :param df:
        :param specific_cols: Receives the list of columns to change in the specified DataFrame
        """
        specific_cols = cls.optional_col(optional, specific_cols, list(df.columns))
        assert type(specific_cols) == list, "The specific_cols variable must be a list"
        list_exclude = [x for x in specific_cols if x not in df.columns.values.tolist()]

        assert len(list_exclude) == 0, "The specific_cols variable: {} must be in the cols_to_consider variable".format(
            list_exclude
        )

        # For each chosen column from the DataFrame, standardize the ints according to the function description
        # May need more corrections
        for col in specific_cols:
            df.loc[:, col] = df[col].fillna('')
            df.loc[:, col] = df[col].astype('str')
            df.loc[:, col] = df[col].str.replace(',', '.')
            df.loc[:, col] = df[col].str.replace(' ', '')
            df.loc[:, col] = df[col].apply(lambda value: value.replace(
                '.', '', value.count('.') - 1) if value.count('.') >= 2 else value)
            df.loc[:, col] = df[col].apply(lambda value: value.split('.')[0] if '.' in value else value)
            df.loc[:, col] = df[col].apply(lambda value: 0 if not value.isdigit() else value)
            df.loc[:, col] = df[col].astype('int64')

        return df

    @classmethod
    def nifs(cls, df: pd.DataFrame, specific_cols: list, optional=0) -> pd.DataFrame:
        """
        This function standardizes nif's in the chosen columns of the given DataFrame according to some criteria:
        - it fills nan's to empty strings;
        - converts the columns to strings, in order to treat nif's as strings before converting them to ints;
        - lower all letters in the string;
        - delete every character that is not a digit or a letter;
        - replace nan's represented as strings;
        - convert the strings to ints.
        :param optional:
        :param df:
        :param specific_cols: Receives the list of columns to change in the specified DataFrame
        """

        assert type(specific_cols) == list, "The specific_cols variable must be a list"
        specific_cols = cls.optional_col(optional, specific_cols, list(df.columns))
        for col in specific_cols:
            df.loc[:, col] = df[col].fillna('0')
            df.loc[:, col] = df[col].astype('str')
            df.loc[:, col] = df[col].str.strip()
            df.loc[:, col] = df[col].str.lower()
            df.loc[:, col] = df[col].apply(lambda value: value.split('.')[0] if '.' in value else value)
            df.loc[:, col] = df[col].apply(lambda value: value.split(',')[0] if ',' in value else value)
            df.loc[:, col] = df[col].str.replace(r'[\W]', '0')
            df.loc[:, col] = df[col].str.replace('nan', '0')
            df.loc[:, col] = df[col].str.replace('Nan', '0')

        return df

    @staticmethod
    def columns_names(df: pd.DataFrame, new_columns_names: list, if_needed=True) -> pd.DataFrame:
        """
        This function changes the columns names of a data frame according to a list given by the user
        :return:
        """

        assert type(new_columns_names) == list, "The specific_cols variable must be a list"

        if if_needed:
            for i in range(len(new_columns_names)):
                new_columns_names[i] = new_columns_names[i].replace('Ç', 'C')
                new_columns_names[i] = re.sub('[^a-zA-Z0-9 \n.]', ' ', new_columns_names[i].lower())
                new_columns_names[i] = new_columns_names[i].lower().replace(' ', '_')

        df.columns = new_columns_names
        return df


class MaskData:
    """
    Class with anonimization functions
    """

    @classmethod
    def unique_id(cls) -> str:
        """
        Creates unique id's
        :return:
        """
        code = uuid.uuid4().hex
        return code

    @classmethod
    def save_json(cls, parent_path: str, core_file_version: dict, file_name: str) -> None:
        """
        Save any json file (type of file of a version
        :param parent_path: path to the a folder the file should be saved
        :param core_file_version: a dict structure with the json template
        :param file_name: name of the file to save
        :return:
        """

        try:
            # Check path again
            assert type(parent_path) == str, "The parent_path must be a string"
            assert os.path.exists(parent_path), "The parent_path directory doesn't exist"
            assert type(file_name) == str, "The file_name must be a string"
            assert type(core_file_version) == dict, "The core_file_version must be a dict"

            path_file = os.path.join(parent_path, file_name)

            write_file = open(path_file, 'w')
            json_string = json.dumps(core_file_version, indent=4)
            write_file.write(json_string)
            write_file.close()

        except Exception as e:
            logger.error(e)
            raise Exception("Not able to create a version file: ", e)

    @classmethod
    def cols(cls, df: pd.DataFrame, **mask_parameters) \
            -> pd.DataFrame:
        """
        Mask in ids
        :param df:
        :param col_to_consider:
        :param path_folder_parent:
        :param key_file_name:
        :return:
        """
        assert 'references_path' in mask_parameters.keys()
        path_folder_parent = mask_parameters['references_path']
        if not os.path.exists(path_folder_parent):
            os.makedirs(path_folder_parent, exist_ok=True)
        assert 'mask_cols' in mask_parameters.keys()
        cols_to_mask = mask_parameters['mask_cols']
        for k in cols_to_mask.keys():
            logger.info("Masking {}".format(k))
            file_name = '{}.json'.format(cols_to_mask[k])
            path_key_file = os.path.join(path_folder_parent, file_name)
            if os.path.exists(path_key_file):
                json_file = open(path_key_file)
                key_file = json.load(json_file)
                json_file.close()
            else:
                key_file = {}

            df["key"] = ""

            for index, row in df.iterrows():
                # Select current variables to mask
                cc_nif = str(row[k])

                if cc_nif not in key_file.keys():
                    # Mask number
                    key_file[cc_nif] = cls.unique_id()

                # Assign new values to each row
                df.at[index, 'key'] = key_file[cc_nif]

            # Change the columns
            df[k] = df["key"]

            # Drop columns
            df = df.drop(["key"], axis=1)

            # Save new pair keys
            cls.save_json(path_folder_parent, key_file, file_name)

        return df


class MaskData:
    """
    Class with anonimization functions
    """

    @classmethod
    def unique_id(cls) -> str:
        """
        Creates unique id's
        :return:
        """
        code = uuid.uuid4().hex
        return code

    @classmethod
    def save_json(cls, parent_path: str, core_file_version: dict, file_name: str) -> None:
        """
        Save any json file (type of file of a version
        :param parent_path: path to the a folder the file should be saved
        :param core_file_version: a dict structure with the json template
        :param file_name: name of the file to save
        :return:
        """

        try:
            # Check path again
            assert type(parent_path) == str, "The parent_path must be a string"
            assert os.path.exists(parent_path), "The parent_path directory doesn't exist"
            assert type(file_name) == str, "The file_name must be a string"
            assert type(core_file_version) == dict, "The core_file_version must be a dict"

            path_file = os.path.join(parent_path, file_name)

            write_file = open(path_file, 'w')
            json_string = json.dumps(core_file_version, indent=4)
            write_file.write(json_string)
            write_file.close()

        except Exception as e:
            logger.error(e)
            raise Exception("Not able to create a version file: ", e)

    @classmethod
    def cols(cls, df: pd.DataFrame, **mask_parameters) \
            -> pd.DataFrame:
        """
        Mask in ids
        :param df:
        :param col_to_consider:
        :param path_folder_parent:
        :param key_file_name:
        :return:
        """
        assert 'references_path' in mask_parameters.keys()
        path_folder_parent = mask_parameters['references_path']
        if not os.path.exists(path_folder_parent):
            os.makedirs(path_folder_parent, exist_ok=True)
        assert 'mask_cols' in mask_parameters.keys()
        cols_to_mask = mask_parameters['mask_cols']
        for k in cols_to_mask.keys():
            logger.info("Masking {}".format(k))
            file_name = '{}.json'.format(cols_to_mask[k])
            path_key_file = os.path.join(path_folder_parent, file_name)
            if os.path.exists(path_key_file):
                json_file = open(path_key_file)
                key_file = json.load(json_file)
                json_file.close()
            else:
                key_file = {}

            df["key"] = ""

            for index, row in df.iterrows():
                # Select current variables to mask
                cc_nif = str(row[k])

                if cc_nif not in key_file.keys():
                    # Mask number
                    key_file[cc_nif] = cls.unique_id()

                # Assign new values to each row
                df.at[index, 'key'] = key_file[cc_nif]

            # Change the columns
            df[k] = df["key"]

            # Drop columns
            df = df.drop(["key"], axis=1)

            # Save new pair keys
            cls.save_json(path_folder_parent, key_file, file_name)

        return df