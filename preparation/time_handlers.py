"""
Scripts that recognizes time references
"""
import logging
from datetime import datetime
from dateutil import relativedelta

logger = logging.getLogger(__name__)
DATE_FORMAT_SOURCE = "%Y%m%d_%H%M%S"


def compare_refresh_rate(old_date: datetime, new_date: datetime, time_reference: str, refresh_rate: int) -> bool:
    """
    Function that compares two dates and returns true if the difference is greater than the refresh_rate
    :param refresh_rate:
    :param new_date:
    :param old_date:
    :param time_reference:
    :return:
    """
    assert time_reference in ['year', 'month', 'week', 'day', 'hour', None], 'Time Reference not valid'
    diff_date = relativedelta.relativedelta(new_date, old_date)
    if time_reference is None:
        return True
    elif time_reference == 'day' and diff_date.days > refresh_rate:
        return True
    elif time_reference == 'week' and diff_date.weeks > refresh_rate:
        return True
    elif time_reference == 'month' and diff_date.months > refresh_rate:
        return True
    elif time_reference == 'year' and diff_date.hours > refresh_rate:
        return True
    elif time_reference == 'hour' and diff_date.hours > refresh_rate:
        return True
    else:
        return False


def select_validate_format(date_string: str, format_list: list) -> str:
    """
    Returns true if the format is valid for the date
    :param format_list: list with formats to test
    :param date_string: date in string
    :return:
    """
    try:
        # validation of input variable
        assert type(date_string) == str, "date_string must be a string"
        assert type(format_list) == list, "format must be a list"
        selected_format = ""
        selected = False
        i = 0
        while not selected:
            f = format_list[i]
            assert type(f) == str, "every element in format_list must be a string"
            try:
                datetime.strptime(date_string, f)
                selected_format = f
                selected = True
            except ValueError:
                i += 1
                if i >= len(format_list):
                    selected = True
        return selected_format
    except AssertionError as e:
        logger.error(e)
        return ""


def map_date(date_object: datetime, ref: str) -> datetime:
    """
    Function that accordingly the team format and a reference will create a date_object
    :param date_object:
    :param ref:
    :return:
    """
    assert type(date_object) == datetime, "The date_object must be a datetime"
    assert type(ref) == str, "The ref must be a string"

    if ref.find('y') == 0 and ref.count('y') == 4:
        to_change_year: int = date_object.year
        ref = ref.replace('yyyy', str(to_change_year))
    if ref.find('m') == 4 and ref.count('m') == 2:
        to_change_month: int = date_object.month
        ref = ref.replace('mm', str(to_change_month))
    if ref.find('d') == 6 and ref.count('d') == 2:
        to_change_day: int = date_object.day
        ref = ref.replace('dd', str(to_change_day))

    return datetime.strptime(ref, DATE_FORMAT_SOURCE)
