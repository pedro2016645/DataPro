"""
Modules that manages the different sources
Improve : Add workspace validation
"""
import logging
import os
import pkgutil
from datetime import datetime

import pandas as pd
import yaml

from ..processing.upload import ReadFiles

logger = logging.getLogger(__name__)


class DirectoryOperations:
    """
    Class for function DirectoryOperations
    """

    @staticmethod
    def select_recent_file(dir_path: str, tag_file: str) -> str:
        """
         Select the most recent files
         :param tag_file:
         :param dir_path:
         :return:
         """
        assert os.path.exists(dir_path), "The {0} doesn't exist".format(dir_path)
        files = os.listdir(dir_path)
        files = [f for f in files if f.startswith(tag_file)]
        files.sort(reverse=True)
        if len(files) > 0:
            return os.path.join(dir_path, files[0])
        else:
            return ''

    @staticmethod
    def select_files(dir_path: str, tag_file: str) -> list:
        """
         Select the most recent files
         :param tag_file:
         :param dir_path:
         :return:
         """

        assert os.path.exists(dir_path), "The {0} doesn't exist".format(dir_path)
        files = os.listdir(dir_path)
        files = [f for f in files if f.startswith(tag_file)]
        return files

    @staticmethod
    def check_dir(dir_path: str, *folder_name: str) -> None:
        """
        Check if already exist a folder
        :param folder_name:
        :param dir_path:
        :return:
        """
        assert type(dir_path) == str, "The dir path must be a string"
        for folder in folder_name:
            dir_path = os.path.join(dir_path, folder)
            if not os.path.exists(dir_path):
                os.mkdir(dir_path)

    @staticmethod
    def select_file_by_date(dir_path: str, tag_file: str, date_file: str) -> str:
        """
        Select the file by date
        :param dir_path:
        :param tag_file:
        :param date_file:
        :return:
        """
        assert type(dir_path) == str, "The {0} doesn't exist".format(dir_path)
        try:
            datetime.strptime(date_file, '%Y%m%d')
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYYMMDD")

        files = os.listdir(dir_path)
        files = [f for f in files if f.startswith(tag_file)]

        files_equal_date = [x for x in files if date_file in x]
        files_equal_date.sort(reverse=True)

        return os.path.join(dir_path, files_equal_date[0])

    @staticmethod
    def select_close_file_by_date(dir_path: str, tag_file: str, lim_date: str, date_format: str = "%Y%m%d_%H%M%S",
                                  structure: str = '{}_{}.csv') -> str:
        """
        Select the file by date
        On structure build the format in order to use tag_file first and then lim_date
        :param dir_path:
        :param lim_date:
        :param date_format:
        :param tag_file:
        :param structure
        :return:
        """
        assert type(dir_path) == str, "The {0} doesn't exist".format(dir_path)

        try:
            datetime.strptime(lim_date, date_format)
        except ValueError:
            raise ValueError("Incorrect data format, should be {}".format(date_format))

        files = os.listdir(dir_path)
        files = [f for f in files if f.startswith(tag_file)]

        # Copy file
        structure = structure.format(tag_file, lim_date)
        files.append(structure)
        files.sort()

        # Cut list on structure located
        lim_list = files.index(structure)

        return os.path.join(dir_path, files[lim_list - 1])


class PackageOperations:
    """
    Class to perform action package depend
    """

    @staticmethod
    def read_source(package_name: str, source_name: str, file_name=None, selection_mode: str = 'recent',
                    filter_date=None) -> tuple or pd.DataFrame:
        """
        Function to read sources from local or other
        :param filter_date:
        :param selection_mode:
        :param source_name:
        :param package_name:
        :param file_name:
        :return:
        """

        try:
            assert type(package_name) == str, "The given package must be a string"
            assert type(selection_mode) == str, "The given selection_mode must be a string"
            assert selection_mode in ['recent', 'by_date'], "The given selection_mode only can be recent or by date "
            if selection_mode == 'recent':
                assert filter_date is None, "The given filter_date must be None"
            else:
                assert type(filter_date) == str, "The given filter_date must be str"
            # TODO: Add option for local package
            try:
                # get data
                # Check if packages are shyness and shad
                if package_name.startswith("shyness") or package_name.startswith("shad"):
                    raw_data = pkgutil.get_data(package_name, '{0}.yaml'.format(source_name))
                    source_params = yaml.load(raw_data.decode('ANSI'), Loader=yaml.FullLoader)
                    # Start the process for shyness
                    if package_name.startswith("shyness"):
                        content_filter = ['processed_source', 'save_parameters']
                        if len(package_name.split('.')) == 3:
                            product = package_name.split('.')[2]
                        else:
                            product = ''
                        source_load_params = source_params[content_filter[0]][content_filter[1]]
                        # Build the path to the file
                        file_path = os.path.join(source_load_params['path'], product, source_load_params['tag'])
                    # Start process for shad
                    else:
                        content_filter = ['prepared_source', 'save_parameters']
                        use_case = package_name.split('.')[2]
                        src_type = package_name.split('.')[3]
                        source_load_params = source_params[content_filter[0]][content_filter[1]]
                        # Build the path to the file
                        file_path = os.path.join(source_load_params['path'], use_case, src_type,
                                                 source_load_params['tag'])
                    # Use of file_name in case the name of the file is different than the name of the tag given in
                    # the yaml
                    if file_name is not None:
                        if type(file_name) == str:
                            file_name = [file_name]
                        elif type(file_name) == list:
                            wrong_parameters = [x for x in file_name if type(x) != str]
                            assert len(wrong_parameters) == 0, "The parameters in file_name should be strings"

                        else:
                            raise NameError("Error in parameter on file_name")
                    # Usual case when the name of the file is the same as the tag
                    else:
                        file_name = [source_load_params['tag']]
                    result = []
                    # When it is given a list of files to extract
                    for i in file_name:
                        # Extract the most recent file
                        if selection_mode == 'recent':
                            source_path = DirectoryOperations.select_recent_file(file_path, i)
                        # Extract a file with a specific date
                        else:
                            source_path = DirectoryOperations.select_file_by_date(file_path, i, filter_date)
                        df = ReadFiles.data_file(source_path, encoding='utf-8-sig', decimal='.', d_type=None)
                        result.append(df)
                    if len(result) == 1:
                        return result[0]
                    else:
                        return tuple(result)

            except ValueError as e:
                logger.error(e)

        except AssertionError as e:
            logger.error(e)
