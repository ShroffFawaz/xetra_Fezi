import os 
import unittest
import boto3 
from datetime import datetime,timedelta

import pandas as pd
from io import BytesIO,StringIO

from xetra.common.s3 import s3Bucketconncetor
from xetra.transformers.xetra_transformation import XetraETL, XetraSourceCofig, XetraTargetConfig
from xetra.common.meta_process import Metaprocess

class IntTestXetraETLMethod(unittest.TestCase):
    def setUp(self):

        #Definging the class arguments
        self.s3_access_key="AWS_ACCESS_KEY_ID"
        self.s3_secret_key="AWS_SECRET_ACCESS_KEY"
        self.s3_endpoint_url='https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name_src='xetra-int-test-sr'
        self.s3_bucket_name_trg='xetra-int-test-tr'
        self.meta_key='meta_file.csv'
        #creating a bucket on the mocked s3
        self.s3=boto3.resource(service_name='s3',
                               endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name_src,
                              CreateBucketConfiguration={'LocationConstraint':'eu-central-1'
                              })
        self.s3.create_bucket(Bucket=self.s3_bucket_name_trg,
                              CreateBucketConfiguration={'LocationConstraint':'eu-central-1'
                              })
        self.scr_bucket=self.s3.Bucket(self.s3_bucket_name_src)
        self.trg_bucket=self.s3.Bucket(self.s3_bucket_name_trg)

        #creating a test instance
        self.s3_bucket_scr=s3Bucketconncetor(self.s3_access_key,
                                              self.s3_secret_key,
                                              self.s3_endpoint_url,
                                              self.s3_bucket_name_src)
        self.s3_bucket_trg=s3Bucketconncetor(self.s3_access_key,
                                              self.s3_secret_key,
                                              self.s3_endpoint_url,
                                              self.s3_bucket_name_trg)
        #creating a list of dates
        self.dates=[(datetime.today().date()-timedelta(days=day)).strftime(Metaprocess.MetaColumns.META_FILE_FORMAT.value) for day in range(8)]
        conf_dict_src = {
            'src_first_extract_date': self.dates[3],
            'src_columns': ['ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume'],
            'src_col_date': 'Date',
            'src_col_isin': 'ISIN',
            'src_col_time': 'Time',
            'src_col_start_price': 'StartPrice',
            'src_col_min_price': 'MinPrice',
            'src_col_max_price': 'MaxPrice',
            'src_col_traded_vol': 'TradedVolume'
        }

        conf_dict_trg = {
            'trg_col_isin': 'isin',
            'trg_col_date': 'date',
            'trg_col_op_price': 'opening_price_eur',
            'trg_col_clos_price': 'closing_price_eur',
            'trg_col_min_price': 'minimum_price_eur',
            'trg_col_max_price': 'maximum_price_eur',
            'trg_col_dail_trad_vol': 'daily_traded_volume',
            'trg_col_ch_prev_clos': 'change_prev_closing_%',
            'trg_key': 'report1/xetra_daily_report1_',
            'trg_key_date_format': '%Y%m%d_%H%M%S',
            'trg_format': 'parquet'
        }
        self.source_config=XetraSourceCofig(**conf_dict_src)
        self.target_config =XetraTargetConfig(**conf_dict_trg)
        #creating Mock Row source Data
        columns_src=['ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume']
        data=[
             ['AT0000A0E9W5', 'SANT', self.dates[8], '12:00', 20.19, 18.45, 18.20, 20.33, 877],
            ['AT0000A0E9W5', 'SANT', self.dates[7], '15:00', 18.27, 21.34, 18.27, 21.34, 987],
            ['AT0000A0E9W5', 'SANT', self.dates[6], '13:00', 20.21, 18.27, 18.21, 20.42, 633],
            ['AT0000A0E9W5', 'SANT', self.dates[5], '14:00', 18.27, 21.19, 18.39, 20.58, 455],
            ['AT0000A0E9W5', 'SANT', self.dates[4], '07:00', 20.58, 19.27, 18.89, 20.58, 9066],
            ['AT0000A0E9W5', 'SANT', self.dates[3], '08:00', 19.27, 21.14, 19.27, 21.14, 1220],
            ['AT0000A0E9W5', 'SANT', self.dates[2], '07:00', 23.58, 23.58, 23.58, 23.58, 1035],
            ['AT0000A0E9W5', 'SANT', self.dates[1], '08:00', 23.58, 24.22, 23.31, 24.34, 1028],
            ['AT0000A0E9W5', 'SANT', self.dates[1], '09:00', 24.22, 22.21, 22.21, 25.01, 1523],
        ]
        #upload date to Mock s3
        self.df_src=pd.DataFrame(data,columns=columns_src)

        self.s3_bucket_scr.write_df_to_s3(self.df_src.loc[0:0], f'{self.dates[5]}/{self.dates[5]}_BINS_XETR12.csv', 'csv')
        self.s3_bucket_scr.write_df_to_s3(self.df_src.loc[1:1], f'{self.dates[4]}/{self.dates[4]}_BINS_XETR14.csv', 'csv')
        self.s3_bucket_scr.write_df_to_s3(self.df_src.loc[2:2], f'{self.dates[3]}/{self.dates[3]}_BINS_XETR13.csv', 'csv')
        self.s3_bucket_scr.write_df_to_s3(self.df_src.loc[3:3], f'{self.dates[2]}/{self.dates[2]}_BINS_XETR14.csv', 'csv')
        self.s3_bucket_scr.write_df_to_s3(self.df_src.loc[4:4], f'{self.dates[2]}/{self.dates[2]}_BINS_XETR07.csv', 'csv')
        self.s3_bucket_scr.write_df_to_s3(self.df_src.loc[5:5], f'{self.dates[1]}/{self.dates[1]}_BINS_XETR08.csv', 'csv')
        self.s3_bucket_scr.write_df_to_s3(self.df_src.loc[6:6], f'{self.dates[1]}/{self.dates[1]}_BINS_XETR07.csv', 'csv')


        columns_report = [
        'ISIN', 'Date', 'opening_price_eur', 'closing_price_eur',
        'MaxPrice_eur', 'MinPrice_eur', 'TradedVolume', 'change_prev_closing_%'
                                                ]

        data_report = [
            ['AT0000A0E9W5', self.dates[3], 20.21, 18.27, 20.58, 18.21, 1088, -14.39],
            ['AT0000A0E9W5', self.dates[2], 20.58, 19.27, 21.14, 18.89, 10286, 5.47],
            ['AT0000A0E9W5', self.dates[1], 23.58, 23.58, 25.01, 22.21, 3586, 22.37],
        ]

        

        
        self.df_report=pd.DataFrame(data_report,columns=columns_report)
    def tearDown(self):
        for key in self.scr_bucket.objects.all():
            key.delete()
        for key in self.trg_bucket.objects.all():
            key.delete()
        

    def test_etl_report(self):
        #exceted Results
        # Expected results
        df_exp=self.df_report
        meta_exp=[self.dates[3], self.dates[3], self.dates[3], self.dates[3]]

        # Method execution
        xetra_etl=XetraETL(self.s3_bucket_scr,self.s3_bucket_trg,self.meta_key,self.source_config,
                               self.target_config)
        xetra_etl.etl_report1()
        trg_file=self.s3_bucket_trg.list_files_in_prefix(self.target_config.trg_key)[0]      
        data=self.trg_bucket.Object(trg_file).get().get('Body').read()
        #coverting it into a pandas dataframe
        out_buffer=BytesIO(data)
        df_result=pd.read_parquet(out_buffer)
        #Read the meta file (from S3)
        meta_file=self.s3_bucket_trg.list_files_in_prefix(self.meta_key)[0]
        df_meta_result=self.s3_bucket_trg.read_to_csv_df(meta_file)
        #Compare the dates in meta file
        self.assertEqual(list(df_meta_result['source_date']),meta_exp)



if __name__=='__main__':
    unittest.main()

    