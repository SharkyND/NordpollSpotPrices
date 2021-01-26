import os
import json
import csv
import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import logging


class SystemConfig:
    """SystemConfig
    """
    class Keys:
        """Keys
        """
        LOG_FILE = "logFile"

    def __init__(self):
        """__init__
        """
        self.is_valid = bool()
        self.log_file = str()

    def __str__(self):
        """__str__
        """
        return "%s: %s\n%s: %s" % (
            self.Keys.LOG_FILE,
            self.log_file,
            "is_valid",
            self.is_valid)

    def parse(self, config):
        """parse
        """
        try:
            self.log_file = os.path.join(os.path.dirname(
                os.path.realpath(__file__)), config[self.Keys.LOG_FILE])
            self.is_valid = True

        except Exception as e:
            print("Error when parsing system configuration: %s" % e)
            self.__init__()
            self.is_valid = False


class IotTicketConfig:
    """IotTicketConfig
    """
    class Keys:
        """Keys
        """
        BASE_URL = "baseUrl"
        USERNAME = "IotTicketUsername"
        PASSWORD = "IotTicketPassword"
        DEVICE_ID = "IotTicketDeviceID"
        MAPPING_FILES = "mappingFiles"
        READ_INTERVAL_SEC = "readIntervalSec"

    def __init__(self):
        """__init__
        """
        self.is_valid = bool()
        self.baseurl = str()
        self.username = str()
        self.password = str()
        self.device_id = str()
        self.mapping_files = list()
        self.device_read_sec = int()
        self.id_to_iot_ticket = dict()

    def __str__(self):
        """__str__
        """
        return "%s: %s\n%s: %s\n%s: %s\n%s: %s\n%s: %s\n%s: %s\n%s: %s" % (
            self.Keys.BASE_URL,
            self.baseurl,
            self.Keys.USERNAME,
            self.username,
            self.Keys.PASSWORD,
            self.password,
            self.Keys.DEVICE_ID,
            self.device_id,
            self.Keys.MAPPING_FILES,
            self.mapping_files,
            self.Keys.READ_INTERVAL_SEC,
            self.device_read_sec,
            "is_valid",
            self.is_valid)

    def parse(self, config):
        """parse
        """
        try:
            self.baseurl = config[self.Keys.BASE_URL]
            self.username = os.environ.get(self.Keys.USERNAME)
            self.password = os.environ.get(self.Keys.PASSWORD)
            self.device_id = os.environ.get(self.Keys.DEVICE_ID)
            self.mapping_files = config[self.Keys.MAPPING_FILES]

            #appPath = os.path.dirname(os.path.realpath(__file__))
            # Getting the connection string
            connect_str = os.getenv('AzureWebJobsStorage')
            # Creating the blob storage client
            blob_service_client = BlobServiceClient.from_connection_string(
                connect_str)
            # The container name which is defined from the start
            container_name = os.getenv("container_name_Mappingfile")

            container_client = blob_service_client.get_container_client(
                container_name)

            blob_list = container_client.list_blobs()
            blob_names = []

            for blob in blob_list:
                blob_names.append(blob.name)

            for blob_name in blob_names:
                blob_client = container_client.get_blob_client(blob_name)
                blob_content = blob_client.download_blob().readall().decode('utf-8-sig')

                self.Test_List = []
                with io.StringIO(blob_content) as mapData:
                    reader = csv.reader(mapData)
                    for row in reader:
                        self.Test_List.append(row[0])

                with io.StringIO(blob_content) as mapData:
                    reader = csv.reader(mapData)
                    self.id_to_iot_ticket.update({row[0].strip(): tuple(r.strip() for r in row[1:])
                                                  for row in reader if row})

            self.device_read_sec = config[self.Keys.READ_INTERVAL_SEC]
            # logging.error(self.id_to_iot_ticket) #extra
            min_read_interval = 300  # allow reading from scheinder at minimum every 5 minutes

            if self.device_read_sec < min_read_interval:
                self.device_read_sec = min_read_interval

            self.is_valid = True

        except Exception as e:
            print("Error when parsing IoT-TICKET configuration: %s" % e)
            self.__init__()
            self.is_valid = False


class SchneiderConfig:
    """SchneiderConfig
    """
    class Keys:
        """Keys
        """
        BASE_URL = "baseUrl"
        USERNAME = "ScneiderUserName"
        PASSWORD = "ScneiderUserPassword"

    def __init__(self):
        """__init__
        """
        self.is_valid = bool()
        self.baseurl = str()
        self.username = str()
        self.password = str()

    def __str__(self):
        """__str__
        """
        return "%s: %s\n%s: %s\n%s: %s\n%s: %s" % (
            self.Keys.BASE_URL,
            self.baseurl,
            self.Keys.USERNAME,
            self.username,
            self.Keys.PASSWORD,
            self.password,
            "is_valid",
            self.is_valid)

    def parse(self, config):
        """parse
        """
        try:
            self.baseurl = config[self.Keys.BASE_URL]
            self.username = os.environ.get(self.Keys.USERNAME)
            self.password = os.environ.get(self.Keys.PASSWORD)
            self.is_valid = True

        except Exception as e:
            print("Error when parsing Schneider configuration: %s" % e)
            self.__init__()
            self.is_valid = False


class Configuration:
    """Configuration
    """
    class Keys:
        """Keys
        """
        SYSTEM_CONFIG = "systemConfig"
        IOT_TICKET_CONFIG = "iotTicketConfig"
        SCHNEIDER_CONFIG = "schneiderConfig"

    def __init__(self):
        """__init__
        """
        self.is_valid = bool()
        self.system = SystemConfig()
        self.iot = IotTicketConfig()
        self.schneider = SchneiderConfig()

    def __str__(self):
        """__str__
        """
        return "%s: \n%s\n\n%s: \n%s\n\n%s: \n%s\n\n%s: %s" % (
            self.Keys.SYSTEM_CONFIG,
            self.system,
            self.Keys.IOT_TICKET_CONFIG,
            self.iot,
            self.Keys.SCHNEIDER_CONFIG,
            self.schneider,
            "is_valid",
            self.is_valid)

    def parse(self, config_file):
        """parse
        """
        try:
            config = json.load(open(config_file))

            systemConf = config[self.Keys.SYSTEM_CONFIG]
            iotTicketConf = config[self.Keys.IOT_TICKET_CONFIG]
            schneiderConf = config[self.Keys.SCHNEIDER_CONFIG]

            self.system.parse(systemConf)
            self.iot.parse(iotTicketConf)
            self.schneider.parse(schneiderConf)
            self.is_valid = self.system.is_valid and \
                self.iot.is_valid and \
                self.schneider.is_valid

        except Exception as e:
            print("Error when parsing configuration: %s" % e)
            self.__init__()
            self.is_valid = False
