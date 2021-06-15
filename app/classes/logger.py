import logging
import pprint
import pandas


class Logger:

    def __init__(self, logging_level):
        # Initializing the logging level according to the user input.
        level = self.__logging_level_number(logging_level)

        logging.basicConfig(format=f'{logging_level}: %(asctime)s: %(message)s \n',
                            datefmt='%m/%d/%Y %I:%M:%S %p',
                            level=level)

    def log_print(self, message, level_name):
        # Sets up a message that you want to display to the logging level that is chosen by the user.
        level_number = self.__logging_level_number(level_name)
        logging.log(level_number, message)

    def log_pprint(self, object_to_print, level_name):
        new_line = '\n'

        # Sets up a object that will be pprinted with the logging level that is chosen by the user.
        level_number = self.__logging_level_number(level_name)

        # Checks to see if the object to be pprinted is a pandas dataframe.
        if type(object_to_print) == pandas.core.frame.DataFrame:
            logging.log(level_number, new_line + object_to_print.to_string())

        else:
            logging.log(level_number, new_line + pprint.pformat(object_to_print, compact=True, width=100))

    def __logging_level_number(self, level):
        # Takes the logging level as a string and returns its equivalent in int format.
        if level == "INFO":
            return 20
        elif level == "DEBUG":
            return 15
        elif level == "Normal":
            return 30
        elif level == "FLAG":
            return 25
