"""
Different Methods to download data
"""
import logging
import os

import pandas as pd
from ..processing.workspace import DirectoryOperations
from datetime import datetime

logger = logging.getLogger(__name__)


class SaveData:
    @classmethod
    def write_file(cls, df: pd.DataFrame, tag: str,
                   date_tag: str, path: str = r'N:\DSI\ASI4\ASI42\Partilha\Data\sources\clean_data',
                   product: str = "master",
                   delimiter: str = ',', encoding: str = 'UTF-8-SIG', file_type: str = 'csv',
                   date_part: str = '') -> None:
        """
        Function to save file in csv
        :param date_part:
        :param file_type:
        :param df:
        :param path:
        :param tag:
        :param date_tag:
        :param delimiter:
        :param encoding:
        :param product:
        :return:
        """
        # Saving clean source
        logger.info('Saving file')
        assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
        assert type(tag) == str, "The tag parameter must be a string"
        assert type(date_tag) == str, "The date_tag parameter must be a string"
        assert type(path) == str, "The path must be a string"
        assert type(delimiter) == str, "The delimiter must be a string"
        assert type(encoding) == str, "The encoding must be a string"
        assert type(file_type) == str, "The file_type must be a string"
        assert type(date_part) == str,  "The date_part must be a string"

        # Check directory path with product
        DirectoryOperations.check_dir(path, product)

        if date_part != '':
            complete_path = os.path.join(path, product, tag)
            DirectoryOperations.check_dir(complete_path, '{}_{}'.format(tag, date_part))
            complete_path = os.path.join(complete_path, '{}_{}'.format(tag, date_part))
            tag = '{}_{}'.format(tag, date_part)
        else:
            # Check directory path with product and source tag
            complete_path = os.path.join(path, product)
            DirectoryOperations.check_dir(complete_path, tag)
            complete_path = os.path.join(complete_path, tag)
        # Check date_tag
        try:
            datetime.strptime(date_tag, "%Y%m%d_%H%M%S")
        except ValueError as e:
            logger.error(e)
            raise ValueError("The given str_time must be on the format: {0}".format("%Y%m%d_%H%M%S"))

        df.to_csv(
            os.path.join(complete_path,
                         tag + '_' + date_tag + '.' + file_type),
            sep=delimiter,
            index=False,
            encoding=encoding)

        logger.info("Source updated")

    @classmethod
    def write_table(cls, df: pd.DataFrame, tag: str, date_tag: str) -> None:
        """
        Function to write a table
        :param df:
        :param tag:
        :param date_tag:
        :return:
        """
        assert type(df) == pd.DataFrame, "The df parameter must be a DataFrame"
        assert type(tag) == str, "The df parameter must be a string"
        assert type(date_tag) == str, "The df parameter must be a string"