import unittest 
from xetra.common.s3 import s3Bucketconncetor
import os
from moto import mock_aws
import boto3
from datetime import datetime
from xetra.common.meta_process import Metaprocess
from io import StringIO
import pandas as pd
from xetra.common.Custom_exceptions import WrongMetaFileException

class TestMetaProcessMethod(unittest.TestCase):
    def setUp(self):
        """
        setting up the environment
        """
        self.mock_s3 = mock_aws()
        self.mock_s3.start()
        #Definging the class arguments
        self.s3_access_key="AWS_ACCESS_KEY_ID"
        self.s3_secret_key="AWS_SECRET_ACCESS_KEY"
        self.s3_endpoint_url='https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name="test-bucket"

        #creating s3 access keys as environment variables
        os.environ[self.s3_access_key]="KEY1"
        os.environ[self.s3_secret_key]="KEY2"

        #creating a bucket on the mocked s3
        self.s3=boto3.resource(service_name='s3',
                               endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={'LocationConstraint':'eu-central-1'
                              })
        self.s3_bucket=self.s3.Bucket(self.s3_bucket_name)

        #creating a test instance 
        self.s3_bucket_meta=s3Bucketconncetor(self.s3_access_key,
                                              self.s3_secret_key,
                                              self.s3_endpoint_url,
                                              self.s3_bucket_name)

    def tearDown(self):
        """executing after unittesting"""
        self.mock_s3.stop()
    #1st testcase 
    def test_update_meta_file_on_meta_file(self):
        #expected results
        date_list_exp=['2022-12-25','2022-12-26']
        proc_date_list_exp=[datetime.today().date()]*2
        #test init
        meta_key='meta.csv'
        #method execution
        Metaprocess.meta_file_update(meta_key,date_list_exp,self.s3_bucket_meta)
        #Read meta file
        data = self.bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer= StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_rest=list(df_meta_result[Metaprocess.MetaColumns.META_SOURCE_DATE_COL.value])
        proc_date_list_result=list(pd.to_datetime(df_meta_result[Metaprocess.MetaColumns.META_PROCESS_COL.value]).dt.date)
        #Test after method execution 
        self.assertEqual(date_list_exp,date_list_rest)
        self.assertEqual(proc_date_list_exp,proc_date_list_result)
        #Cleaning after key
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[                
                {'Key':meta_key}
                ]
            }
        )
    def test_update_meta_file_empty_dat_list(self):
        #expected results
        retrun_exp=True
        log_exp='The dataframe is empty! no file will be written'
        # test init
        date_list=[]
        meta_key='meta.csv'
        #method excutions
        with self.assertLogs() as logm:
            result=Metaprocess.meta_file_update(date_list,meta_key,self.s3_bucket_meta)
            self.assertIn(log_exp,logm.output[1])
        self.assertEqual(log_exp,result)
    def test_update_meta_file_meta_file_ok(self):
        #exepected resutls
        date_list_old=['2022-12-25','2022-12-26']
        date_list_new=['2022-12-27','2022-12-28']
        date_list_exp=date_list_new + date_list_old
        proc_date_list_exp=[datetime.today().date()]*4
        #test iniit
        meta_key='meta.csv'
        meta_content=(
            f'{Metaprocess.MetaColumns.META_SOURCE_DATE_COL.value}'
            f'{Metaprocess.MetaColumns.META_PROCESS_COL.value}\n'
            f'{date_list_old[0]}'
            f'{datetime.today().strftime(Metaprocess.MetaColumns.META_PROCESS_DATE_FORMAT.value)}\n'
            f'{date_list_new[1]}'
            f'{datetime.today().strftime(Metaprocess.MetaColumns.META_PROCESS_DATE_FORMAT.value)}\n'
        )
        self.s3_bucket.put_object(Body=meta_content,key=meta_key)
        #Method execuation 
        Metaprocess.meta_file_update(date_list_exp,meta_key,self.s3_bucket_meta)
        #Read mete file
        data = self.bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer= StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_rest=list(df_meta_result[Metaprocess.MetaColumns.META_SOURCE_DATE_COL.value])
        proc_date_list_result=list(pd.to_datetime(df_meta_result[Metaprocess.MetaColumns.META_PROCESS_COL.value]).dt.date)
        #Test after method execution 
        self.assertEqual(date_list_exp,date_list_rest)
        self.assertEqual(proc_date_list_exp,proc_date_list_result)
        #Cleaning after key
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[                
                {'Key':meta_key}
                ]
            }
        )
    def test_update_meta_file_meta_file_wrong(self):
                #exepected resutls
        date_list_old=['2022-12-25','2022-12-26']
        date_list_new=['2022-12-27','2022-12-28']
        date_list_exp=date_list_new + date_list_old
        proc_date_list_exp=[datetime.today().date()]*4
        #test iniit
        meta_key='meta.csv'
        meta_content=(
            f'wrong_file{Metaprocess.MetaColumns.META_PROCESS_COL.value}\n'
            f'{date_list_old[0]}'
            f'{datetime.today().strftime(Metaprocess.MetaColumns.META_PROCESS_DATE_FORMAT.value)}\n'
            f'{date_list_new[1]}'
            f'{datetime.today().strftime(Metaprocess.MetaColumns.META_PROCESS_DATE_FORMAT.value)}\n'
        )
        self.s3_bucket.put_object(Body=meta_content,key=meta_key)
        #Method execuation 
        Metaprocess.meta_file_update(date_list_exp,meta_key,self.s3_bucket_meta)
        with self.assertRaises(WrongMetaFileException):
            Metaprocess.meta_file_update(date_list_exp,meta_key,self.s3_bucket_meta)
        #Cleaning after key
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[                
                {'Key':meta_key}
                ]
            }
        )
if __name__=='__main__':
    unittest.main()