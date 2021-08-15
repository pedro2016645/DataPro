"""
Module that has function to alter a query
"""
import logging
from datetime import datetime
from ..preparation.time_handlers import map_date

logger = logging.getLogger(__name__)


class QueryComponents:

    @staticmethod
    def change_time_reference(query: str, **query_params) -> str:
        """
        Change time reference on a query
        :param query: string with query
        :param query_params:
        :return:
        """
        logger.info(query_params)
        if 'time' in query_params.keys():
            logger.info("Changing time references on query")
            assert 'date_obj' in query_params['time'].keys(), 'Invalid query_params on params'
            time_params = query_params['time']
            date_obj = query_params['time']['date_obj']

            assert type(date_obj) == datetime, 'Invalid query_params on params'
            valid_keys = list(time_params.keys())
            valid_keys.remove('date_obj')
            for tag in valid_keys:
                if 'ref' in time_params[tag].keys():
                    # Date obj must be changed
                    date_mapped_obj = map_date(date_obj, time_params[tag]['ref'])
                    date_tag = date_mapped_obj.strftime(time_params[tag]['format'])
                else:
                    date_tag = date_obj.strftime(time_params[tag]['format'])
                tag = '[{}]'.format(tag)
                query = query.replace(tag, "'{}'".format(date_tag))

        return query

    @staticmethod
    def add_source_aux_path(query: str, **query_params) -> str:
        """
        Function that allows to insert into local files on queries
        :param query: string with query
        :param query_params:
        """
        logger.info("Adding path on insert into")
        if 'insert_into' in query_params.keys():
            for temp_file_tag in query_params['insert_into']:
                tag = '[{}]'.format(temp_file_tag)
                try:
                    assert 'temp_file' in query_params['insert_into'][temp_file_tag].keys(), \
                        "temp_file parameter is missing for the temp_file_tag {}".format(temp_file_tag)
                    query = query.replace(tag,
                                          "'{}'".format(query_params['insert_into'][temp_file_tag]['temp_file']))
                    if 'create_table' in query_params['insert_into']['temp_file'].keys():
                        # Build create query
                        assert 'queries_cols_name' in query_params['insert_into']['temp_file']['create_table'].keys(), \
                            "The queries_cols_name in query_params"
                        assert 'queries_cols_type' in query_params['insert_into']['temp_file']['create_table'].keys(), \
                            "The queries_cols_type in query_params"
                        cols_name = query_params['insert_into']['temp_file']['create_table']['queries_cols_name']
                        cols_type = query_params['insert_into']['temp_file']['create_table']['queries_cols_type']
                        assert type(cols_name) == str, "The queries_cols_type must be a string"
                        assert type(cols_type) == str, "The queries_cols_type must be a string"
                        cols_name = cols_name.split(',')
                        cols_type = cols_type.split(',')

                        assert len(cols_type) == len(cols_name), "The cols_type and cols_name must be the same size"
                        create_table = ""
                        for i in range(len(cols_name)):
                            if i < len(cols_name) - 1:
                                create_table += "{} {},\n".format(cols_name[i], cols_type[i])
                            else:
                                create_table += "{} {}".format(cols_name[i], cols_type[i])
                        logger.info(create_table)
                        query = query.replace('[create_table]', create_table)
                except Exception as e:
                    logger.error(e)

        return query

    @staticmethod
    def select_universe(query: str, **query_params) -> str:
        """
        Function that allows to select the entities universe making a right join
        :param query: string with query
        :param query_params:
        """
        logger.info("Selecting universe clients")
        tag = '[{}]'.format('select_universe')
        if query_params['select_universe'] is not None:
            try:
                assert 'selected_universe_table' in query_params['select_universe'].keys()
                assert 'join_on' in query_params['select_universe'].keys()
                assert 'main_table' in query_params['select_universe'].keys()
                assert 'type_join' in query_params['select_universe'].keys()
                assert type(query_params['select_universe']['join_on']) == list
                subquery = "{} [selected_universe_table] on ".format(query_params['select_universe']['type_join'])
                # Replace table to do right join
                subquery = subquery.replace("[selected_universe_table]",
                                            query_params['select_universe']['selected_universe_table'])

                # Build list of to join on
                on_subquery = ""
                join_on = query_params['select_universe']['join_on']
                main_table = query_params['select_universe']['main_table']
                selected_clients = query_params['select_universe']['selected_universe_table']
                for i in range(len(join_on)):
                    combo = join_on[i].split(":")
                    on_subquery += "{}.{}={}.{}".format(main_table, combo[0], selected_clients, combo[1])
                    if i < len(join_on) - 1:
                        on_subquery += " and "
                subquery = "{}{}".format(subquery, on_subquery)
                query = query.replace(tag, "{}".format(subquery))

            except Exception as e:
                logger.error(e)
                query = query.replace(tag, "")

        else:
            query = query.replace(tag, "")
        return query

    @staticmethod
    def substitute_values(query: str, **query_params) -> str:
        """
       Function that allows to insert some parameter in the query
       :param query: string with query
       :param query_params:
       """
        logger.info("Substituting values in the query.")
        if 'substitute_values' in query_params.keys():
            for val in query_params['substitute_values'].keys():
                tag = '[{}]'.format(val)
                query = query.replace(tag, "{}".format(query_params['substitute_values'][val]))

        return query

    @staticmethod
    def update_date_part(query: str, **query_params) -> str:
        """
       Function that allows change the type of date part we want on a query
       :param query: string with query
       :param query_params:
       """
        logger.info("Changing date_part subquery on query.")

        try:
            # Check input variables
            assert "time_groups" in query_params.keys(), "Time groups is not set on the yaml source"
            assert type(query) == str, "The variable query is not a string"
            valid_date_parts = ["MONTH", "WEEK", "DAY", 'DOY']
            assert "date_part" in query_params['time_groups'].keys(), \
                "Date Part is not set on the time_groups on the yaml source"
            date_part = query_params['time_groups']['date_part']
            assert date_part in valid_date_parts, "The given date_part is not valid"

            # Replace the date part
            tag = "[{}]".format("date_part")
            query = query.replace(tag, "'{}'".format(date_part))

            return query
        except AssertionError as e:
            logger.error(e)

    @staticmethod
    def update_time_difference(query: str, **query_params) -> str:
        """
       Function that allows to get the corresponding time difference from date part
       :param query: string with query
       :param query_params:
       """
        logger.info("Changing date_part subquery on query.")

        try:
            # Check input variables
            assert "time_difference" in query_params.keys(), "Time difference is not set on the yaml source"
            assert type(query) == str, "The variable query is not a string"
            # Dict with the correspondence between date_part and its respective interval
            valid_interval_dif = {"MONTH": "month", "WEEK": "days", "DAY": "day", "DOY": "day"}
            # Possible variable units in interval function
            possible_intervals = ['year', 'month', 'day', 'hour', 'minute', 'second', 'years', 'months', 'days',
                                  'hours', 'minutes', 'seconds']

            # Check input variables
            assert "interval_dif" in query_params['time_difference'].keys(), \
                "The interval_dif is not set on the time_difference on the yaml source"
            assert "interval_t" in query_params['time_difference'].keys(), \
                "The interval_t is not set on the time_difference on the yaml source"

            # Get interval difference
            interval_dif = query_params['time_difference']['interval_dif']
            assert type(interval_dif) == str, "The variable interval_dif is not a string"

            # Get time constant that will be multiplied by the interval difference
            interval_t = query_params['time_difference']['interval_t']
            assert type(interval_t) == int, "The variable interval_dif is not an integer"

            # Check if the interval difference corresponds to the date_part
            if interval_dif == 'date_part':
                assert "time_groups" in query_params.keys(), "Time groups is not set on the yaml source"
                assert type(query) == str, "The variable query is not a string"
                valid_date_parts = ["MONTH", "WEEK", "DAY", 'DOY']
                assert "date_part" in query_params['time_groups'].keys(), \
                    "Date Part is not set on the time_groups on the yaml source"
                date_part = query_params['time_groups']['date_part']

                interval_dif = valid_interval_dif[date_part]
                assert date_part in valid_date_parts, "The given date_part is not valid"

                # In case of 'WEEK' than the number of days should be multiplied by 7 which corresponds to 1 week
                if date_part == 'WEEK':
                    interval_t = interval_t * 7
            assert interval_dif in possible_intervals, "The given interval_dif is not valid"

            # Replace the interval difference
            tag = "[{}]".format("interval_dif")
            query = query.replace(tag, "'{} {}'".format(interval_t, interval_dif))

            return query
        except AssertionError as e:
            logger.error(e)


QUERY_MAPPING = {
    'time': QueryComponents.change_time_reference,
    'insert_into': QueryComponents.add_source_aux_path,
    'select_universe': QueryComponents.select_universe,
    'substitute_values': QueryComponents.substitute_values,
    "time_groups": QueryComponents.update_date_part,
    'time_difference': QueryComponents.update_time_difference
}
