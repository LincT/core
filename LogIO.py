import datetime
import logging
import os

__logging_config__ = None
__logger__ = None
__name__ = None
__file_name__ = None
__log_level_dict = None


class LogHandler:

    def __init__(self, file_name, name="", logging_level: str = "debug"):
        self.__logger = logging.getLogger(name=name)
        self.__level = str(logging_level)

        log_level_dict = {
            "info": logging.INFO,
            "warning": logging.WARNING,
            "warn": logging.WARNING,
            "debug": logging.DEBUG,
            "critical": logging.CRITICAL,
            "error": logging.ERROR
        }
        self.__log_level_dict = log_level_dict
        # -# for how many levels up to step, -0 is effectively same directory.,
        # holdover from the last project this class was used in.
        directory = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-0])
        full_log_file = os.path.join(directory, file_name)
        self.__file_name__ = full_log_file
        if logging_level in log_level_dict.keys():

            self.__logging_config = \
                logging.basicConfig(level=log_level_dict[self.__level], filename=full_log_file, filemode="a")

        else:
            self.__logging_config = \
                logging.basicConfig(level=logging.DEBUG, filename=full_log_file, filemode="a")

    def write(self, data: str, level: str = "", file_name: str = None):
        """
        :param data:
        :param level:
        :param file_name:
        :return:
        """
        data = "{}\t{}".format(self.timestamp(), data)
        if level:
            write_level = str(level)
        else:
            write_level = str(self.__level)

        if write_level == "debug":
            print("values:\ndata: {}\nlevel: {}".format(data, level))

        level_dict = {
            "info": self.__logger.info,
            "warning": self.__logger.warning,
            "warn": self.__logger.warning,
            "debug": self.__logger.debug,
            "critical": self.__logger.critical,
            "exception": self.__logger.exception,
            "error": self.__logger.error
        }

        if file_name:

            """
            need to get new file name to write to,
            temporarily set to new file name
            write data to new file
            revert back to old file name for future logging
            currently can at least change data written to log if filename added
            WIP, not fully functional
            """
            # old_dir = self.__file_name__
            # directory = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-4])
            # full_log_file = os.path.join(directory, "logs", file_name)
            # print("full file name:\t{}".format(full_log_file))
            # self.__logging_config = logging.basicConfig(level=logging.DEBUG, filename=full_log_file, filemode="a")
            # level_dict[write_level](data)
            #
            # if write_level in level_dict.keys():
            #     level_dict[write_level](data)
            # else:
            #     message = "log level not specified, adding message as info:\n\t{}".format(data)
            #     self.__logger.debug(message)
            # self.__logging_config = logging.basicConfig(level=logging.DEBUG, filename=old_dir, filemode="a")
            level_dict["info"]("\t{}\n\t{}".format(file_name, data))
        else:
            if write_level in level_dict.keys():
                level_dict[write_level](data)
            else:
                message = "log level not specified, adding message as info:\n\t{}".format(data)
                self.__logger.debug(message)

    def adjust_logging_level(self, level: str):

        if level in self.__log_level_dict:
            self.__logging_config = \
                logging.basicConfig(level=self.__log_level_dict[self.__level],filemode="a")

    @staticmethod
    def timestamp():
        return "UTC: " + str(datetime.datetime.utcnow())
