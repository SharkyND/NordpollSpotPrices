import os
import sys
import json
import datetime

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
    
class Logger:
    """Logger
    """
    class Keys:
        TIME    = "time"
        TYPE    = "type"
        MESSAGE = "message"
        INFO    = "INFO"
        ERROR   = "ERROR"
        DEBUG   = "DEBUG"


    def __init__(self, log_file, print_message=True):
        """__init__
        """
        self._log_file      = log_file
        self._print_message = print_message

    
    def _get_time(self):
        """_get_time
        """
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


    def _trim_message(self, message):
        """_trim_message
        """
        message = str(message)
        message = message.translate({ord(c): None for c in "\n\r\t\"\'"})
        return message


    def _log_message(self, message, message_type):
        """_log_message
        """
        msg = dict()

        if self._print_message:
            print(message)

        msg[self.Keys.TIME]    = self._get_time()
        msg[self.Keys.TYPE]    = message_type
        msg[self.Keys.MESSAGE] = self._trim_message(message)
        
        j = json.dumps(msg, sort_keys=True)

        with open(self._log_file, "a") as log_file:
            log_file.write(j + "\n")
    

    def log_info(self, message):
        """log_info
        """
        self._log_message(message, self.Keys.INFO)
    

    def log_error(self, message):
        """log_error
        """
        self._log_message(message, self.Keys.ERROR)


    def log_debug(self, message):
        """log_debug
        """
        self._log_message(message, self.Keys.DEBUG)
