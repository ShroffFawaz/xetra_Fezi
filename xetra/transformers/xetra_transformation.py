"""Xetra ETL Component"""
from typing import NamedTuple
import logging
from xetra.common.s3 import s3Bucketconncetor

class XetraSourceCofig(NamedTuple):
    """
    Class for source configuration data

    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volumne in source
    """
    src_first_extract_date:str
    src_columns:list
    src_col_date:str
    src_col_isin:str  
    src_col_time:str
    src_col_start_price:str
    src_col_min_price:str
    src_col_max_price:str
    src_col_traded_vol:str

class XetraTargetConfig(NamedTuple):

    """
    Class for target configuration data

    trg_col_isin: column name for isin in target
    trg_col_date: column name for date in target
    trg_col_op_price: column name for opening price in target
    trg_col_clos_price: column name for closing price in target
    trg_col_min_price: column name for minimum price in target
    trg_col_max_price: column name for maximum price in target
    trg_col_dail_trad_vol: column name for daily traded volume in target
    trg_col_ch_prev_clos: column name for change to previous day's closing price in target
    trg_key: basic key of target file
    trg_key_date_format: date format of target file key
    trg_format: file format of the target file
    """
    trg_col_isin:str
    trg_col_date:str
    trg_col_op_price:str
    trg_col_clos_price:str
    trg_col_min_price:str
    trg_col_max_price:str
    trg_col_dail_trad_vol:str
    trg_col_ch_prev_clos:str
    trg_key:str
    trg_key_date_format:str
    trg_format:str
class XetraETL(NamedTuple):
    def __init__(self,s3_bucket_src:s3Bucketconncetor
                 ,s3_bucket_trg:s3Bucketconncetor,meta_key:str,
                 srg_args:XetraSourceCofig,trg_args:XetraTargetConfig,meta_update_list,extract_date_list,extract_date):
        self._logger=logging.getLogger(__name__)
        self.s3_bucket_src=s3_bucket_src
        self.s3_bucket_trg=s3_bucket_trg
        self.meta_key=meta_key
        self.srg_args=srg_args
        self.trg_args=trg_args
        self.extract_date=extract_date
        self.extract_date_list=extract_date_list
        self.meta_update_list=meta_update_list
        
    def extract(self):
        pass
    def transform(self):
        pass
    def load(self):
        pass
    def etl_report1(self):
        pass
