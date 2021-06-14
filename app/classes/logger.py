import logging
import pprint


class Logger:

    def __init__(self, logging_level):
        # Initializing the logging level according to the user input.
        level = self.__logging_level_number(logging_level)
        logging.basicConfig(format=f'{logging_level}: %(asctime)s: %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p',
                            level=level)

    def log_print(self, message, level):
        # Sets up a message that you want to display to the logging level that is chosen by the user.
        level_number = self.__logging_level_number(level)
        logging.log(level_number, message)

    def log_pprint(self, object_to_print, level):
        # Sets up a object that will be pprinted with the logging level that is chosen by the user.
        level_number = self.__logging_level_number(level)

        # Coverts the object to be printed in a pprint format.
        things_to_pprint = pprint.pformat(object_to_print)

        # Each line is printed at a time.
        for line in things_to_pprint.split('\n'):
            logging.log(level_number, line)

    def __logging_level_number(self, level):
        # Takes the logging level as a string and returns its equivalent in int format.
        if level == "INFO":
            return 20
        elif level == "DEBUG":
            return 10
        elif level == "Normal":
            return 30
