"""Xetra ETL Component"""
from typing import NamedTuple
import logging
from xetra.common.s3 import s3Bucketconncetor
import pandas as pd
from datetime import datetime,timedelta
from xetra.common.meta_process import Metaprocess



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
class XetraETL:
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
        self.meta_update_list=[date for  date in self.extract_date_list 
                               if date>=self.extract_date]
        
    def extract(self):
        self._logger.info('Extracting Xetra source file started !')
        files=[key for date in self.extract_date_list
                    for key in self.s3_bucket_src.list_files_in_prefix(date)]
        if not files:
            data_frame=pd.DataFrame()
        else:
            data_frame=pd.concat([self.s3_bucket_src.read_to_csv_df(file)
                                 for file in files],ignore_index=True)
        self._logger.info('Extracting Xetra source file is finished......')
        return data_frame
    def transform(self,data_frame:pd.DataFrame):
        """
        Applies the necessary transformation to create report 1
        :param data_frame: Pandas DataFrame as Input
        :returns:
        data_frame: Transformed Pandas DataFrame as Output
        """
        if data_frame.empty:
            self._logger.info('The dataframe is empty. No transformations will be applied.')
            return data_frame
        self._logger.info('Applying transformations to Xetra source data for report 1 started...')
        # Filtering necessary source columns
        data_frame=data_frame.loc[self.srg_args.src_columns]
        # Removing the missing values
        data_frame.dropna(inplace=True)
        #
        data_frame['opening_price']=data_frame.sort_values('Date').groupby(['ISIN','Date'])['StartPrice'].transform('first')
        data_frame['closing_price']=data_frame.sort_values('Date').groupby(['ISIN','Date'])['EndPrice'].transform('first')
        data_frame=data_frame.groupby(['ISIN','Date'],as_index=False).agg(opening_price_eur=('opening_price','min'),closing_price_eur=('closing_price','min'),MaxPrice_eur=('MaxPrice','max'),MinPrice_eur=('MinPrice','min'),TradedVolume=('TradedVolume','sum'))
        data_frame['prev_closing_price']=data_frame.sort_values(by=['Date']).groupby(['ISIN'])['closing_price_eur'].shift(1)
        data_frame['change_prev_closing_%']=(data_frame['closing_price_eur'] - data_frame['prev_closing_price'])/data_frame['prev_closing_price']*100
        data_frame.drop(columns='prev_closing_price',inplace=True)
        data_frame=data_frame.round(decimals=2)
        data_frame = data_frame[data_frame.Date >= self.extract_date].reset_index(drop=True)
        self._logger.info('Transformation for report 1 finished.')
        return data_frame

    def load(self,data_frame:pd.DataFrame):
        """
        Saves a Pandas DataFrame to the target

        :param data_frame: Pandas DataFrame as Input
        """
        # Creating target key
        target_key=(
            f'{self.trg_args.trg_key}'
            f'{datetime.today().strftime(self.trg_args.trg_key_date_format)}'
            f'{self.trg_args.trg_format}'
        )
        #Savingh the dataframe to s3
        self.s3_bucket_trg.write_df_to_s3(data_frame,target_key,self.trg_args.trg_format)
        self._logger.info('Xetra target data successfully written.')
        #update the meta file
        Metaprocess.meta_file_update(self.meta_update_list,self.meta_key,self.s3_bucket_trg)
        self._logger.info('Xetra meta file successfully updated.')
        return True

    def etl_report1(self):
        #Extraction
        data_frame=self.extract()
        #Transforming
        data_frame =self.transform(data_frame)
        #Loading
        data_frame=self.load(data_frame)
