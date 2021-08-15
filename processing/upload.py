"""
Different Methods to upload data
"""
import logging
import os
import re
import pandas as pd
from ..processing.connectors import NetezzaConn

logger = logging.getLogger(__name__)


class ReadFiles:

    @classmethod
    def excel(cls, path: str = "./../Confirming_facturas2018.xlsx", sheet_number: int = 0, header_row: int = 0,
              multilevel: bool = False, macro_tags: list = None, micro_tags: list = None,
              duplicated_macro_tag: str = None, change_name_init_cols=None, decimal=",") -> \
            pd.DataFrame:
        """
        Import excel files
        :param duplicated_macro_tag:
        :param change_name_init_cols:
        :param decimal:
        :param header_row:
        :param sheet_number:
        :param path:path to the file
        :param multilevel: Receives a boolean, if True the excel have multilevel header that will be processed by
        __change_names_multilevel_header function
        :param macro_tags: Receives a list with tags referent to upper_level
        :param micro_tags: Receives a list with new tags to attribute

        :return: return a dataframe with a one sheet information
        """

        assert type(multilevel) == bool, 'The multilevel parameter must be a boolean.'

        if change_name_init_cols is not None:
            assert type(change_name_init_cols) == list, "The change_name_init_cols must be a list"
            pd_content = pd.read_excel(path, sheet_number, header=header_row,
                                       names=change_name_init_cols, dtype=object)
        else:
            pd_content = pd.read_excel(path, sheet_number, header=header_row, dtype=object)

        if multilevel:
            assert macro_tags is not None, 'Must give the parameter macro_tags'
            assert micro_tags is not None, 'Must give the parameter micro_tags'

            pd_content = cls.change_names_multilevel_header(pd_content,
                                                            macro_tags,
                                                            micro_tags,
                                                            duplicated_macro_tag)

        return pd_content

    @staticmethod
    def data_file(path: str = "./../Confirming_facturas2018", header_row: int = 0, delimiter: str = ",",
                  encoding: str = 'utf', quote='"', decimal='.', change_name_init_cols=None, d_type=object) -> pd.DataFrame:
        """
        Import txt files
        :param change_name_init_cols:
        :param decimal:lol
        :param quote:
        :param encoding:
        :param delimiter:
        :param header_row:
        :param path: path to the file
        :param d_type:
        :return: return a dataframe with a txt file content
        """

        if change_name_init_cols is not None:
            assert type(change_name_init_cols) == list, "The change_name_init_cols must be a list"
            pd_content = pd.read_csv(path, header=header_row, delimiter=delimiter, encoding=encoding,
                                     quotechar=quote, error_bad_lines=False, decimal=decimal, low_memory=False,
                                     names=change_name_init_cols, dtype=d_type)
        else:
            pd_content = pd.read_csv(path, header=header_row, delimiter=delimiter, encoding=encoding,
                                     quotechar=quote, error_bad_lines=False, decimal=decimal, low_memory=False,
                                     dtype=d_type)
        return pd_content

    @staticmethod
    def raw(path_file: str = "", file_encoding: str = "ANSI") -> list:
        """
        Reads a raw file and save its lines to a list
        :param path_file: absolute path/ relative path to the file
        :param file_encoding: the origin encoding of the file
        :return: a list with all the lines
        """

        assert type(path_file) == str, "The path_file must be a str"
        assert os.path.exists(path_file), "The given path must exist"
        assert type(file_encoding) == str, "The file_encoding must be a str"

        logger.info("The parameters were accepted")
        logger.info("Started reading the file")

        # Open File
        file_content = open(path_file, encoding=file_encoding)

        # Read all files of the file
        lines = file_content.readlines()
        logger.info("Finished reading the file")

        file_content.close()
        return lines

    @staticmethod
    def table(query_path: str, columns_name: list, package: str, user: str, password: str,
              env: str = 'dev', **query_params) -> pd.DataFrame:
        """
        Reads content from database
        :param package:
        :param columns_name:
        :param query_path:
        :param user:
        :param password:
        :param env:
        :param query_params: parameters to change the query
        :return:
        """

        conn = NetezzaConn(user, password, env)
        conn.create()
        df = conn.select_query(query_path, package, columns_name, **query_params)
        return df

    @staticmethod
    def remove_unnamed(df: pd.DataFrame) -> pd.DataFrame:
        """
        Reads content from database
        :param df:
        :return:
        """

        col_to_remove = [x for x in df.columns if 'unnamed' in x.lower()]
        df = df.drop(columns=col_to_remove)

        return df

    @classmethod
    def change_names_multilevel_header(cls, df: pd.DataFrame, macro_tags: list, micro_tags: list,
                                       duplicated_macro_tag: str = None) -> pd.DataFrame:
        """
        Change the column names from a multilevel header DataFrame
        :param duplicated_macro_tag:
        :param df: Receives a DataFrame with multilevel header
        :param macro_tags: Receives a list with tags referent to upper_level
        :param micro_tags: Receives a list with new tags to attribute
        :return: DataFrame with a header with column names with the format: micro_tag + column_name
        """
        assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
        assert type(macro_tags) == list, "The macro_tags must be a list"
        assert type(micro_tags) == list, "The micro_tags must be a list"

        assert len(macro_tags) == len(micro_tags), "The micro_tags length must be equal to the macro_tag length"

        selected_columns = []
        selected_columns_original = []
        original_columns = df.columns.tolist()
        named_columns = [x for x in original_columns if not x.startswith("Unnamed")]

        for i in range(len(macro_tags)):
            tag = macro_tags[i]
            micro_tag = micro_tags[i]
            columns_with_tags = [x for x in named_columns if x.startswith(tag)]
            # if exists more than a header starting with tag then search by a year and concatenate that information
            # to the end of the column name
            if (len(columns_with_tags) > 1) and (duplicated_macro_tag is not None):
                assert type(duplicated_macro_tag) == str, "The duplicated_macro_tag must be a string"
                # find the year in the headers
                # dup_tag = []
                for col in columns_with_tags:
                    if duplicated_macro_tag == 'year':
                        year = re.findall("\d{4,}", col)
                        # if we can't find a sequence of 4 digits the dup_tag = ''
                        # if have more than one sequence of 4 digits we choose the last one
                        if len(year) == 0:
                            dup_tag = ''
                        else:
                            dup_tag = ''.join(year[-1])
                    else:
                        dup_tag = ''
                    index_start = original_columns.index(col)
                    named_col_index = named_columns.index(col)

                    if named_col_index == len(named_columns) - 1:
                        index_stop = len(original_columns)
                    else:
                        index_stop = original_columns.index(named_columns[named_col_index + 1])

                    tagged_columns = ['{0}_{1}_{2}'.format(micro_tag, x, dup_tag)
                                      for x in df.loc[0][
                                               index_start:index_stop
                                               ].values.tolist()]

                    selected_columns.extend(tagged_columns)
                    selected_columns_original.extend(original_columns[index_start:index_stop])

            elif len(columns_with_tags) > 0:
                index_start = original_columns.index(columns_with_tags[-1])
                named_col_index = named_columns.index(columns_with_tags[-1])

                if named_col_index == len(named_columns) - 1:
                    index_stop = len(original_columns)
                else:
                    index_stop = original_columns.index(named_columns[named_col_index + 1])

                # Add tag
                tagged_columns = ['{0}_{1}'.format(micro_tag, x) for x in df.loc[0][
                                                                          index_start:index_stop
                                                                          ].values.tolist()]

                selected_columns.extend(tagged_columns)
                selected_columns_original.extend(original_columns[index_start:index_stop])

        df = df[selected_columns_original]
        df.columns = selected_columns
        df = df.drop(df.index[0])

        return df
