__all__ = ["s3"]

import logging
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
from curl_cffi import requests
from io import BytesIO

load_dotenv()

class s3:
    client = None
    BUCKET = os.getenv("bucket")
    print(BUCKET)
    def __init__(self):
        config = load_dotenv(".env")
        self.client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key"),
        )
    def show_buckets(self):
        response = self.client.list_buckets()
        for bucket in response['Buckets']:
            print(f'{bucket["Name"]}')
            
    '''
    function to upload file to s3
    file_name: path of file to upload
    bucket: bucket name
    object_name: name of object to be stored in s3 with path
    
    '''
    def upload_file(self, file_name,object_name):
        try:
            response = self.client.upload_file(file_name, self.BUCKET, object_name)
            print("uploaded successfully")
        except ClientError as e:
            logging.error(e)
            return False
    '''
    function to show files in bucket
    bucket: bucket name
    '''   
    def show_files(self, bucket):
        response = self.client.list_objects(Bucket=bucket)
        for file in response['Contents']:
            print(f'{file["Key"]}')

    '''
    checking if the file exists in bucket
    '''
    def check_file(self , file_name): 
        try: 
            response = self.client.head_object(Bucket=self.BUCKET, Key=file_name )
            return 200
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("File is not uploaded")
                return 404
            else:
                print("Error occurred:", e)
                return -1
    '''
    downloading file from bucket 
    '''
    def download_file(self , object_name , file_name):
        try: 
            self.client.download_file(self.BUCKET , object_name , file_name)
            return "successfully downloded"
        except Exception as err: 
            return f"got an err : {err}"
        
    '''
    opening file from bucket 
    and using it 
    '''
    def open_file(self , file_name , object_name): 
        try: 
            with open(file_name , 'wb') as f:
                self.client.download_fileobj(self.BUCKET , object_name ,f )
                return f
        except Exception as err: 
            return f"got an err: {err}"
        
    '''
    directly uploading from url
    '''
    def direct_upload(self, filename , url): 
        try: 
            r = requests.get(url , impersonate="chrome")
            # print(r.content)
            file_stream = BytesIO(r.content)
            self.client.upload_fileobj(file_stream , self.BUCKET , filename )
            print("uploaded successfully")
        except Exception as err: 
            print("error uploading file directly \n" , err)
            raise err
    def search_file(self , file_name): 
        try: 
            response = self.client.list_objects(Bucket=self.BUCKET)
            for file in response['Contents']:
                if file['Key'] == file_name:
                    return 200
            return 404
        except Exception as err: 
            return f"got an err: {err}"




# s3 = s3()
# # print(s3.upload_file("data/2_2024_21_37_50781_Order_26-Apr-2024.pdf.pdf", s3.BUCKET,'data/file1.pdf'))
# # print(s3.show_files(s3.BUCKET))
# print(s3.open_file(file_name="data.pdf",object_name="confo_pdfs/CC_149_2021.pdf"))