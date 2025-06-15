"""connector and method to accessing s3"""
import os
import logging
import boto3
import boto3.session

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
        
        return:
          files list all the file names contain the perfix in the key 
        """
            
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files   
    def read_to_csv_df(self):
        pass
    def write_df_to_s3(self):
        pass

