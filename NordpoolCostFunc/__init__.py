import logging
import os
import sys
import time
import datetime
import subprocess
import requests
import json
import urllib
import dateutil.parser
import calendar
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import urllib
import numpy as np
import urllib.request
import urllib
import xml.etree.ElementTree as ET
import json
import pprint
import os
import sys
import pandas as pd
from dateutil import parser
from dateutil import tz
from pytz import timezone
import datetime
from datetime import timedelta
from dotenv import load_dotenv
load_dotenv()

import azure.functions as func
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(__file__))
from .iotticket.models import datanodesvalue
from .iotticket.client import Client
#from utils import Logger
from configuration import Configuration
from configuration import IotTicketConfig
from cost_extractor import Nordpool



def isfloat(x):
    try:
        float(x)
    except ValueError:
        return False
    else:
        return True

def isint(x):
    try:
        a = float(x)
        b = int(a)
    except ValueError:
        return False
    else:
        return a == b

def get_data_type(value):
    if isinstance(value, bool):
        data_type = "boolean"
    elif isinstance(value, int):
        data_type = "long"
    elif isinstance(value, float):
        data_type = "double"
    elif isinstance(value, str):
        data_type = "string"
    else:
        data_type = "string"

    return data_type

def get_iot_datanode(name, path, value, timeMs):

    nv = datanodesvalue()
    nv.crit = []

    if len(path) > 1:
        nv.set_path(path)

    # For now let's send all the numeric values as float to server
    if isfloat(value):
        value_tmp = float(value)
    else:
        value_tmp = value

    nv.set_name(name)
    # For now lets send everything as float values
    nv.set_dataType(get_data_type(value_tmp))
    nv.set_value(value_tmp)

    if timeMs is None or timeMs == 0:
        nv.set_timestamp(int(datetime.datetime.now().timestamp()*1000))
    else:
        nv.set_timestamp(timeMs)

    return nv

def send_data_in_patches(client, deviceId, data, patch_size=50):
	index = 0
	data_size = len(data)

	while index < data_size:
		
		if index > 0:
			time.sleep(2)
		
		end = index + patch_size
			
		try:
			client.writedata(deviceId, *data[index:end])
		except Exception as e:
			logging.error("Exception while sending data into IoT-TICKET")
			raise
		
		index += patch_size

def send_error_log_to_iot_ticket(client, deviceId, msg, timeMs=0):
    val = get_iot_datanode("Error log", "", msg, timeMs)
    client.writedata(deviceId, val)

def send_error_bit_to_iot_ticket(client, deviceId, state, timeMs=0):
    val = get_iot_datanode("Error active", "", state, timeMs)
    client.writedata(deviceId, val)


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    logging.info("Call the NordPool Class")
    dataCollections=Nordpool(2020)
    
    logging.info("Collecting data for IoT Ticket")

    dataToSend=[]

    for index, row in (dataCollections.dataCollection.iterrows()):
        path="/"
        name="NordpoolSpotPrice"
        value=row["Value"]
        timeMsUTC=row["Time"]
        dataToSend.append(get_iot_datanode(name, path,value, timeMsUTC))

    #Send the data to IoT Ticket:
    timeNow = int(datetime.datetime.now().timestamp() * 1000)
    logging.info("Sending the data to IoT Ticket")

    client = Client(os.getenv("Wapice_baseUrl"), os.getenv("Wapice_username"), os.getenv("Wapice_password"), True) #Change this later

    if len(dataToSend) > 0:
        logging.info("Starting to send data into IoT-TICKET, sending {} values in total".format(len(dataToSend)))
        send_data_in_patches(client, os.getenv("Wapice_deviceId"), dataToSend)
        timeAfterSend = int(datetime.datetime.now().timestamp()*1000)
        logging.info("Sending data into IoT-TICKET took: {} milliseconds".format(timeAfterSend-timeNow))
    
    logging.info("Function Completed")
