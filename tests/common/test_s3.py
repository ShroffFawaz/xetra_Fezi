import os 
import unittest 
import boto3
from moto import mock_aws
from xetra.common.s3 import s3Bucketconncetor

class TestS3BucketConnectorMethod(unittest.TestCase):
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
        self.s3_bucket_conn=s3Bucketconncetor(self.s3_access_key,
                                              self.s3_secret_key,
                                              self.s3_endpoint_url,
                                              self.s3_bucket_name)

    def tearDown(self):
        """executing after unittesting"""
        self.mock_s3.stop()
    def test_list_files_in_prefix_ok(self):
        """
        test the list_file_in_prefix method in case of a worng or not existing prefix
        """
        #expected results
        prefix_exp='prefix/'
        key1_exp=f'{prefix_exp}test1.csv'
        key2_exp=f'{prefix_exp}test2.csv'
        #Test init
        csv_results='''col1,col2,valA,valB'''
        self.s3_bucket.put_object(Body=csv_results,Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_results,Key=key2_exp)
        #Method execution
        list_result=self.s3_bucket_conn.list_files_in_prefix(prefix_exp) 
        #Test after method execution
        self.assertEqual(len(list_result),2)
        self.assertIn(key1_exp,list_result)
        self.assertIn(key2_exp,list_result)
        #cleanup  after Tests
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[                
                {'Key':key1_exp},
               {'Key':key2_exp}
                ]
            }
        )

        
    def test_list_files_in_prefix_wrong_prefix(self):
        """
        test the list_file_in_prefix method in case of a worng or not existing prefix
        """
        #expected results
        prefix_exp='no-prefix/'
        list_result=self.s3_bucket_conn.list_files_in_prefix(prefix_exp) 
        self.assertTrue(not list_result)

if __name__=='__main__':
    unittest.main()

