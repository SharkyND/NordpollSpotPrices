import pandas as pd
import requests
from bs4 import BeautifulSoup
from pytz import timezone
from datetime import datetime
from dateutil import tz
import logging
from blobstorageManupilator import BlobSample


class Nordpool():

    def __init__(self,yearToCall):
        self.yearToCall=yearToCall
        self.URL="https://www.nordpoolgroup.com/48f902/globalassets/marketdata-excel-files/elspot-prices_{}_hourly_eur.xls".format(str(self.yearToCall))
        self.gettingData()
        self.changeDate()

    def callBlobFunction(self):
        blob=BlobSample()
        return blob
    def callDateFromBlob(self):
        blob=self.callBlobFunction() #to initiate
        file_data=blob.getBlobData()
        #with open("DEST_FILE.txt", "r") as my_blob:
        #    file_data = my_blob.readlines()[0]
        return file_data,blob

    def _isNumber(self,number):
        typeOf=type(number)
        if type(number) != int:
            raise Exception("Needs to be an integer")
    
    def initiazPandas(self):
        return pd.DataFrame(columns=['Time','Value'])

    def gettingHTML(self):
        #self._isNumber(self.yearToCall)
        logging.info("Getting data")
        html_text = requests.get(self.URL).text
        soup = BeautifulSoup(html_text, 'html.parser')
        logging.info("Done Getting Data")
        
        return soup
    
    def dateParser(self,datetimeFromData):
        return datetime.strptime(datetimeFromData, '%d-%m-%Y %H:%M:%S')
    
    def datetoEpochUTC(self,datetimeUNIT,timeZone):
        time=timezone(timeZone).localize(datetimeUNIT)
        return int(time.astimezone(tz.gettz('UTC')).timestamp())*1000
    
    def insertDataInPandas(self,series,timestamp,value):
        Object={'Time':[timestamp],'Value':[value]}
        new_series=pd.DataFrame(data=Object)
        series=pd.concat([series,new_series],ignore_index = True)
        return series
    

    def gettingData(self):
        soup=self.gettingHTML()
        #Collecting data
        self.dataCollection=self.initiazPandas()
        #now get the date store in the blob
        timeFromBlob=(self.callDateFromBlob())
        timeFromBlob=self.dateParser(timeFromBlob[0])
        logging.info("Stuffing Pandas")

        for count,row in enumerate(soup.find_all('tr')[3:3000]):
            elements=row.find_all("td")
            #Date
            date=elements[0].text
            #Time
            time=elements[1].text.split("-")[0][:2]+":00:00"
            #datetime
            properdate=self.dateParser(date+" "+time)
            
            

            if (properdate>timeFromBlob):
                self.dataCollection=self.insertDataInPandas(self.dataCollection,self.datetoEpochUTC(properdate,"Europe/Helsinki"),elements[6].text.replace(",","."))
            
            if(count==len(soup.find_all('tr')[3:3000])-1):
                self.lastDateOnDataBase=date+" "+time
        logging.info("Data Collected for {} many points".format(len(self.dataCollection["Time"])))
        #return dataCollection
    
    def changeDate(self):
        blob=self.callBlobFunction()
        logging.info("Time changed to this {}".format(self.lastDateOnDataBase))
        #blob.modifyFile(newDate.strftime('%Y-%m-%dT%H:%M:%SZ'))
        blob.writeBlob(self.lastDateOnDataBase)


            
