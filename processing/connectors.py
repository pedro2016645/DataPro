"""
Module with classes/object that handles the connection with the database, and also executes the queries
"""
import logging

import pandas as pd
import pyodbc
from ..processing.sources_configuration_files import read_sql
from ..processing.query_factory import *

logger = logging.getLogger(__name__)


class NetezzaConn:
    """
    Connector to the Netezza
    """

    def __init__(self, user: str, password: str, env: str = 'dev'):
        """
        Constructor for the object
        """

        #  Read local env file
        assert type(user) == str, 'The user parameter must be a string'
        assert type(password) == str, 'The password parameter must be a string'
        assert env in ['dev', 'prod'], 'The environment parameter must be dev or prod'
        self._user = user
        self._password = password
        self._host = None
        self._database_name = None
        self._connector = None
        self._port = '5480'
        self._env = env

    # Getter and Setter for the objects
    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, new_user):
        self._user = new_user

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, new_password):
        self._password = new_password

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, new_host):
        self._host = new_host

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, new_port):
        self._port = new_port

    @property
    def database_name(self):
        return self._database_name

    @database_name.setter
    def database_name(self, new_database_name):
        self._database_name = new_database_name

    def create(self):
        """
        Create the connection to make querys directly to the database
        :return:
        """
        assert self._env in ['dev', 'prod'], " The environment given is not valid"

        if self._env == 'dev':
            self.host = 'dvqldpuredata'
            self.database_name = 'BDPDDW'
        else:
            self.host = 'puredata'
            self.database_name = 'BDPPDW'
        url_connection = "SERVER={0};PORT={1};DATABASE={2}; UID={3};PWD={4};".format(self.host, self.port,
                                                                                     self.database_name, self.user,
                                                                                     self.password)
        self._connector = pyodbc.connect("DRIVER={NetezzaSQL};" + url_connection)

    def select_query(self, query_to_execute, package: str, final_columns: list, convert_to_data: bool = True,
                     run_test: bool = False, **query_params):
        """
        Executes select querys
        :param package:
        :param final_columns:
        :param convert_to_data: if you only want to execute the query
        :param query_to_execute:
        :param test
        :return:
        """
        result = None
        if type(query_to_execute) != list:
            query_to_execute = [query_to_execute]

        complete_query = ""
        for i in range(len(query_to_execute)):
            q = query_to_execute[i]
            if ":" in q:
                q = q.split(":")
                logger.info(q)
                logger.info(package)
                query = read_sql(q[1], "{}.{}".format('shyness.queries', q[0]))
            else:
                query = read_sql(q, "{}.{}".format('shyness.queries', package))
            # remove last statement except on the last one
            # when using select, the table is no longer accessible
            if 'apply_on_{}'.format(i+1) in query_params.keys():
                for key in query_params['apply_on_{}'.format(i+1)].keys():
                    query = QUERY_MAPPING[key](query, **query_params['apply_on_{}'.format(i+1)])

            if i < len(query_to_execute) - 1:
                query = query.split(";")[:-2]
                query = ";".join(query)
                query += ";"
            complete_query = "{}\n{}".format(complete_query, query)

        query_to_execute = complete_query[:]

        if 'apply_on_all' in query_params.keys():
            for key in query_params['apply_on_all'].keys():
                query_to_execute = QUERY_MAPPING[key](query_to_execute, **query_params['apply_on_all'])

        # Check if is many query
        queries = query_to_execute.split(";")
        try:
            if len(queries) > 1:
                if len(queries[-1]) < 2:
                    queries = queries[:-1]
                queries = [x + ";" for x in queries]
                cursor = self._connector.cursor()
                for query in queries:
                    logger.info("Executing query: {}".format(query))
                    cursor.execute(query)

            else:
                cursor = self._connector.cursor()
                cursor.execute(query_to_execute)
            result = cursor.fetchall()

            if convert_to_data:
                data = []
                for row in result:
                    data.append(list(row))
                result = pd.DataFrame(data, columns=final_columns)
            else:
                result = []
            cursor.close()

        except pyodbc.Error as e:
            logger.error(e)

        return result

    def select_simple_query(self, query_to_execute):
        """
        Executes select query
        :param query_to_execute:
        :return:
        """
        result = []
        try:
            cursor = self._connector.cursor()
            cursor.execute(query_to_execute)
            result = cursor.fetchall()
            data = []
            for row in result:
                data.append(list(row))
            result = pd.DataFrame(data)
            cursor.close()

        except pyodbc.Error as e:
            logger.error(e)

        return result
