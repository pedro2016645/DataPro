"""
Modules that assure quality on the selected sources and corrects some files according to business rules
1. Needs to only run if there's no new files
2. Add a function to save the file, with a proper timestamp
"""
import logging
import os
import re

import pandas as pd
from ..processing.upload import ReadFiles

logger = logging.getLogger(__name__)

END_EMAILS = ['.com', '.pt', '.eu', '.biz', '.es', '.org', '.net', '.de']


class CorrectionFunctions:

    @staticmethod
    def phone_number(value_to_check: str) -> list:

        # Find non digits elements
        number_bites = []
        numbers = ''
        for char in value_to_check:
            if char.isdigit():
                numbers += char

            elif not char.isdigit():
                if len(numbers) > 8:
                    number_bites.append(numbers)
                    numbers = ''
        if len(numbers) > 0:
            number_bites.append(numbers)
        if len(number_bites) > 1 and len(number_bites[-1]) < 9:
            number_bites[-1] = number_bites[-2][:-len(number_bites[-1])] + number_bites[-1]
        elif len(number_bites) > 0 and len(number_bites[-1]) < 9:
            number_bites = number_bites[:-1]

        # Add symbol for foreign numbers
        for i in range(len(number_bites)):
            nb = number_bites[i]
            if len(nb) > 9:
                nb = '+' + nb
                number_bites[i] = nb
        return number_bites

    @staticmethod
    def emails(value_to_check: str) -> list:

        email_bites = []
        email_parts = value_to_check.split('@')
        i = 1
        while i < len(email_parts):
            bite = email_parts[i].lower().replace(' ', '').replace('\t', '')
            end_email = []
            pos_end = []

            for x in END_EMAILS:
                pos = bite.find(x)
                if pos > 0 and x not in end_email:
                    end_email.append(x)
                    pos_end.append(pos)

            if len(end_email) > 0:
                # Build first part of the email
                first_bite = email_parts[i - 1].lower().replace(' ', '')
                first_bite = first_bite[re.compile("[^\W]").search(first_bite).start():]

                only_permitted_characters = [char for char in first_bite if char.isascii()]
                first_bite = ''.join(only_permitted_characters)

                # Build second part of the email
                final_end = end_email[pos_end.index(max(pos_end))]
                final_end_pos = bite.find(final_end) + len(final_end)
                final_end = bite[:final_end_pos]
                to_add = '{}@{}'.format(first_bite, final_end)
                if len(first_bite) > 0:
                    email_bites.append(to_add)

                # Verify if it is another email on bite
                if final_end_pos < len(bite):
                    email_parts[i] = bite[final_end_pos:]
                    i += 1
                else:
                    i += 2
            else:
                i += 2

        return email_bites


class FileCorrections:
    """
    Object that contains functions to correct files
    """

    def __init__(self, quality_params: dict, load_params: dict, header) -> None:

        self._type_source = None
        self._file_lines = None
        self._errors_count_lines = 0
        self._corrected_lines = None
        self._header_cols = None
        self._email_position = None
        self._phone_number_position = None
        self._type_contact = None
        self._id_position = None
        self._name_position = None
        self._content_after_name = None

        # Get variables from environment
        self._parent_folder = load_params['path']
        self._file_name = load_params['file_name'] + '.' + load_params['file_type']
        self._file_encoding = load_params['encoding']
        self._delimiter = load_params['delimiter']
        self._escape_char = load_params['special_char']

        # Check if the source has a phone number or email
        self._email_position = quality_params['email_position']
        self._phone_number_position = quality_params['phone_position']
        self._id_position = quality_params['id_position']
        self._name_position = quality_params['name_position']
        self._content_after_name = quality_params['possible_content_after_name']
        if self._content_after_name is not None:
            self._content_after_name = self._content_after_name.split(",")

        logger.info("All Parameters loaded")

        self._file_lines = ReadFiles.raw(os.path.join(self._parent_folder, self._file_name),
                                         self._file_encoding)
        self._header_cols = header.split(",")

    @property
    def type_source(self) -> str:
        return self._type_source

    @type_source.setter
    def type_source(self, new_type_source: str) -> None:
        """

        :param new_type_source:
        :return:
        """
        assert type(new_type_source) == str, "The new_type_source must be a str"
        assert new_type_source in ['DOCUMENTS', 'CONTRACTS', 'SUPPLIERS'], "The new_type_source is not a valid tag"
        self._type_source = new_type_source

    def adjust_delimiters(self):
        """
        Detects delimiters as characters and detect row with a wrong numbers of columns
        :return:
        """
        header = self._file_lines[0].split(self._delimiter)
        self._corrected_lines = self._file_lines[:]
        nr_cols_accepted = len(header)

        logger.info("Started Process to correct number of columns by delimiters inside escape char")
        for i in range(1, len(self._file_lines)):
            special = [i for i, letter in enumerate(self._file_lines[i]) if letter == self._escape_char]
            line = self._file_lines[i]
            if len(special) >= 2:
                for s in range(0, len(special) - 1, 2):
                    line = line[:special[s]] + line[special[s]:special[s + 1]].replace(self._delimiter, ""
                                                                                       ) + line[special[s + 1]:]

                self._corrected_lines[i] = line

            content = line.split(self._delimiter)
            if len(content) != nr_cols_accepted:
                logger.error(line)
                self._errors_count_lines += 1

        if self._errors_count_lines > 0:
            logger.warning("Found {} error to correct".format(self._errors_count_lines))

        logger.info("Process Ended")

    def number_of_columns_by_contacts(self) -> None:
        """
        Corrects the number of columns per line, so that every line has the same number of columns
        :return:
        """

        assert self._email_position is not None, "This source doesn't have an email column"
        assert self._phone_number_position is not None, "This source doesn't have a phone number column"
        assert self._id_position is not None,  "This source doesn't have a id column"

        # Copy lines of the document
        logger.info("Correction of number of columns has started")

        # Init dictionary to storage contacts for each entity with errors
        dict_error = {}
        for index in range(1, len(self._corrected_lines)):
            list_lines = self._corrected_lines[index].split(self._delimiter)

            # Check if the number of elements of the current line is the same as the header
            make_the_difference = len(list_lines) - len(self._header_cols)
            if make_the_difference > 0:
                # if the line has more columns than the header, stores the entity as key on the dict
                entity_id = list_lines[self._id_position]
                if entity_id not in dict_error.keys():
                    dict_error[entity_id] = {'present': [],
                                             'not_present': []}

                # Start cycle to eliminate the number of columns that exceeded
                for i in range(make_the_difference):
                    if '@' not in list_lines[self._email_position]:
                        # Check if the column doesn't
                        list_lines[self._email_position] = list_lines[
                            self._email_position].replace(' ', '')
                        if list_lines[self._email_position].isdigit():
                            # if the element is a number stores it on the dictionary and removes the cell
                            to_remove_phone_number = list_lines[self._email_position]
                            if to_remove_phone_number not in dict_error[entity_id]['present']:
                                dict_error[entity_id]['not_present'].append(to_remove_phone_number)
                            list_lines.pop(self._email_position)

                # Store last phone number
                remaining_phone_number = list_lines[self._phone_number_position]
                if len(remaining_phone_number) > 0:
                    if remaining_phone_number not in dict_error[entity_id]['present']:
                        dict_error[entity_id]['present'].append(list_lines[self._phone_number_position])

                # Stores new lines
                for number in dict_error[entity_id]['not_present']:
                    to_add_line = list_lines
                    to_add_line[self._phone_number_position] = number
                    self._corrected_lines.append(self._delimiter.join(to_add_line))
                    # logger.info("Added new line: {}".format(self._delimiter.join(to_add_line).strip()))

                dict_error[entity_id]['present'].extend(dict_error[entity_id]['not_present'])
                dict_error[entity_id]['not_present'] = []

            # Replaces the corrected line
            self._corrected_lines[index] = self._delimiter.join(list_lines)
            self._errors_count_lines -= 1
        if self._errors_count_lines > 0:
            logger.critical("Still remaining {} errors".format(self._errors_count_lines))
        else:
            logger.info("No errors remaining")

        logger.info("The process has ended")

    def phone_number(self) -> None:
        """
         Corrects the phone numbers:
         -Deletes blank spaces;
         -Deletes the first and/or last characters if these are not digits;
         -Deletes everything from the first letter (for cases where the phone numbers are separated by "ou" or similar);
         -Deletes contacts with less than 9 digits;
         -Separates different phone numbers, that are distinguished by bars ("/"), in different lines;
         -Deletes symbols;
         -Adds a '+' to the beginning of phone numbers with more than 9 digits.
         :type tag_contact:
         :return:
         """

        assert self._email_position is not None, "This source doesn't have an email column"
        assert self._phone_number_position is not None, "This source doesn't have a phone number column"
        logger.info("Phone number correction process has started")

        dict_contact = {}
        for index in range(1, len(self._corrected_lines)):
            list_suppliers_lines = self._corrected_lines[index].split(self._delimiter)
            number_to_check = list_suppliers_lines[self._phone_number_position]
            list_numbers = CorrectionFunctions.phone_number(number_to_check)
            if len(list_numbers) == 1:
                list_suppliers_lines[self._phone_number_position] = list_numbers[0]
            elif len(list_numbers) > 1:
                id_entity = list_suppliers_lines[0]
                if id_entity not in dict_contact.keys():
                    dict_contact[id_entity] = []
                list_suppliers_lines[self._phone_number_position] = list_numbers[0]
                dict_contact[id_entity].append(list_numbers[0])
                for extra_number in list_numbers[1:]:
                    if extra_number not in dict_contact[id_entity]:
                        extra_line = list_suppliers_lines[:]
                        extra_line[self._phone_number_position] = extra_number
                        self._corrected_lines.append(self._delimiter.join(extra_line))

                        #logger.info("Added new line: {}".format(self._delimiter.join(extra_line).strip()))
                        dict_contact[id_entity].append(extra_number)
            self._corrected_lines[index] = self._delimiter.join(list_suppliers_lines)

    def email(self) -> None:
        """
        Corrects the emails:
        -Deletes rare characters;
        -Deletes the string if it has no '@';
        -Lowers the string;
        -Separates different emails, that are distinguished by spaces (' ') or bars ("/"), in different lines;
        -Separates different emails that have no direct separation;
        -Only leave digits and/or letters in the beginning and end of the string.
        :return:
        """

        assert self._email_position is not None, "This source doesn't have an email column"
        assert self._phone_number_position is not None, "This source doesn't have a phone number column"
        assert self._id_position is not None, "This source doesn't have a id column"

        logger.info("Email correction process has started")

        dict_emails = {}
        for index in range(1, len(self._corrected_lines)):
            list_lines = self._corrected_lines[index].split(self._delimiter)
            email_pos = list_lines[self._email_position]
            emails = CorrectionFunctions.emails(email_pos)
            if len(emails) == 1:
                list_lines[self._email_position] = emails[0]
            elif len(emails) > 1:
                id_entity = list_lines[self._id_position]
                if id_entity not in dict_emails.keys():
                    dict_emails[id_entity] = []
                list_lines[self._email_position] = emails[0]
                dict_emails[id_entity].append(emails[0])
                for extra_email in emails[1:]:
                    if extra_email not in dict_emails[id_entity]:
                        extra_line = list_lines[:]
                        extra_line[self._email_position] = extra_email
                        self._corrected_lines.append(self._delimiter.join(extra_line))
                        dict_emails[id_entity].append(extra_email)
            else:
                list_lines[self._email_position] = ''
            self._corrected_lines[index] = self._delimiter.join(list_lines)

    def last_adjustment(self, line: list) -> list:
        if line[self._name_position + 1] not in self._content_after_name:
            line[self._name_position] = '{0} {1}'.format(line[self._name_position], line[self._name_position + 1])
            line.pop(self._name_position + 1)

        return line

    def save_to_df(self) -> pd.DataFrame:
        self.adjust_delimiters()
        self.number_of_columns_by_contacts()
        self.phone_number()
        self.email()
        for index in range(0, len(self._corrected_lines)):
            line = self._corrected_lines[index].strip().split(self._delimiter)
            if len(line) > 26:
                line = self.last_adjustment(line)
            self._corrected_lines[index] = line

        df = pd.DataFrame(self._corrected_lines[1:], columns=self._header_cols)

        return df
