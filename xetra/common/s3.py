"""connector and method to accessing s3"""
import os
import logging
import boto3
import boto3.session
import pandas as pd
import boto3
from io import StringIO,BytesIO
from typing import Union
from datetime import datetime,timedelta  
from enum import Enum
from xetra.common.Custom_exceptions import WrongFormatException 



class S3FileType(Enum):
    CSV='csv'
    PARQUET='parquet'
class s3Bucketconncetor():
    """
    classes for interacting with s3
    """
    def __init__(self,access_key:str,secret_key:str,endpoint_url:str,bucket:str):
        """
        param access_key:acesss key for accessing s3
        param secret_key:secret key for accessing s3
        param endpoint_ulr:endpoint url for s3
        param bucket: s3 bucket name
        """
        self._logger=logging.getLogger(__name__)
        self.endpoint_url=endpoint_url
        self.session=boto3.Session(aws_access_key_id=os.environ[access_key],
                                   aws_secret_access_key=os.environ[secret_key])
        self._s3=self.session.resource(service_name='s3',endpoint_url=endpoint_url)
        self._bucket=self._s3.Bucket(bucket)
    
    def list_files_in_prefix(self,prefix:str):        
        """
        listing all the perfix files of s3 bucket

        :param:perfix on the s3 bucket that should be filterd with
        
        return:files list all the file names contain the perfix in the key 
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files   
    def read_to_csv_df(self, key:str, sep=',', decoding='utf-8'):
        self._logger.info(f"Read file {self.endpoint_url}/{self._bucket.name}/{key}")
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        date_frame = pd.read_csv(data, delimiter=sep)
        return date_frame
    
    
    def write_df_to_s3(self,data_frame:pd.DataFrame, key:str,file_format:str):
        """
        writing a Pandas DataFrame to S3    
        supported formats: .csv, .parquet

        :data_frame: Pandas DataFrame that should be written
        :key: target key of the saved file
        :file_format: format of the saved file
        """
        if data_frame.empty:
            self._logger.info('The dataframe is empty! No file will be written!')
            return None
        if file_format==S3FileType.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self._put_object(out_buffer,key)
        if file_format == S3FileType.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self._put_object(out_buffer,key)
        self._logger.info('The file format %s is not supported to be written to s3!',file_format)
        raise WrongFormatException
    def _put_object(self, out_buffer:Union[StringIO ,BytesIO], key:str):
        """
        Helper function for self.write_df_to_s3()
        :out_buffer: StringIO | BytesIO that should be written
        :key: target key of the saved file
        """
        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True

