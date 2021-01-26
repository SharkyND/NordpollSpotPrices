import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
load_dotenv()

class BlobSample():
    def __init__(self):
        from azure.storage.blob import BlobServiceClient
        self.contianerName=os.getenv('Container_Name')
        self.fileName=os.getenv('File_Name')
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.contianerName)
        self.blob_client = self.container_client.get_blob_client(self.fileName)
        #self.getBlobData() #This will automatically get the file
    
    """def getFile(self):
        with open(DEST_FILE, "wb") as my_blob:
                download_stream = blob_client.download_blob()
                my_blob.write(download_stream.readall())"""
    
    def _getAllContainers(self):
        listContainers=self.blob_service_client.list_containers()
        allContainers=[]
        [allContainers.append(x.name) for x in listContainers]
        return allContainers
    
    def _delteBlob(self):
        self.blob_client.delete_blob()
    
    def getBlobData(self):
        allContainers=self._getAllContainers()
        if self.contianerName in allContainers: 
            #with open("DEST_FILE.txt", "wb") as my_blob:
            #    download_stream = self.blob_client.download_blob()
            #    my_blob.write(download_stream.readall())
            return (self.blob_client.download_blob().readall()).decode('utf-8')

        else:
            raise Exception("Container NOT found")

    def readFile(self,fileName):
        with open(fileName, 'r') as filename:
            file_data = filename.readlines()
        return file_data

    def modifyFile(self,data):
        f = open("DEST_FILE.txt", "w")
        f.write(data)
        f.close()
        #with open("DEST_FILE", "wb") as my_blob:
        #    my_blob.write(data)

    def writeBlob(self,data):
        self._delteBlob()
        #data = self.readFile("DEST_FILE.txt")
        self.blob_client.upload_blob(data)
    


#sample = BlobSample()
#list_response = sample.getAllContainers()
#print(list_response)
#sample.delteBlob()
#sample.writeBlob()
