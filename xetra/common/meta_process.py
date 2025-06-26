import collections
from enum import Enum
from xetra.common.s3 import s3Bucketconncetor
import pandas as pd
from datetime import datetime,timedelta 
from collections import Counter
from xetra.common.Custom_exceptions import WrongMetaFileException
"""
Methods for processing meta files
"""


class Metaprocess():
    """
    classes for working with meta files
    """
    class MetaColumns(Enum):
            META_SOURCE_DATE_COL='source_date'
            META_PROCESS_COL='process_col'
            META_PROCESS_DATE_FORMAT='%y-%m-%d'
            META_FILE_FORMAT='csv'

    @staticmethod
    def meta_file_update(meta_key:str,extract_date_list:list,s3_bucket_meta:s3Bucketconncetor):
        # Create an Empty DataFrame with Correct Column's
        df_new=pd.DataFrame(columns=[Metaprocess.MetaColumns.META_SOURCE_DATE_COL.value,Metaprocess.MetaColumns.META_PROCESS_COL.value])
        #Fill New DataFrame with Current Info
        df_new[Metaprocess.MetaColumns.META_SOURCE_DATE_COL.value]=extract_date_list
        df_new[Metaprocess.MetaColumns.META_PROCESS_COL.value]=datetime.today().strftime(Metaprocess.MetaColumns.META_FILE_FORMAT.value)
        # Try Reading Existing Meta File
        try:
            df_old=s3_bucket_meta.write_df_to_s3(meta_key)
            if collections.Counter(df_new.columns) != collections.Counter(df_old.columns):
                raise WrongMetaFileException
            df_all=pd.concat([df_old,df_new])
        except s3_bucket_meta.session.client('s3').exceptions.Nosuchkey:
            df_all=df_new
        s3_bucket_meta.write_df_to_s3(df_all,meta_key,Metaprocess.MetaColumns.META_FILE_FORMAT.value)    

    @staticmethod
    def return_date_list(first_date:str,meta_key:str,s3_bucket_meta:s3Bucketconncetor):
        
        min_date = datetime.strptime(first_date, Metaprocess.MetaColumns.META_PROCESS_DATE_FORMAT.value).date() - timedelta(days=1)
        today=datetime.today().date()
        try:
            df_meta=s3_bucket_meta.read_to_csv_df( meta_key)
            dates = [(min_date + timedelta(days=x)) for x in range (0,(today-min_date).days+1 )]
            scr_date=set(pd.to_datetime(df_meta['source_date']).dt.date)
            date_missing=set(dates[1:])-scr_date
            if date_missing:
                min_date=min(set(dates[1:])-scr_date) - timedelta(days=1)
                return_dates=[date.strftime(Metaprocess.MetaColumns.META_PROCESS_DATE_FORMAT.value) for date in dates if date>=min_date]
                return_min_dates=(min_date+timedelta(days=1)).strftime(Metaprocess.MetaColumns.META_PROCESS_DATE_FORMAT.value)
            else:
               return_dates=[]
            return_min_dates=datetime(2200,1,1).date()
        except s3_bucket_meta.session.client('s3').execptions.NoSuchKey:
            return_dates = [(min_date + timedelta(days=x)).strftime(Metaprocess.MetaColumns.META_PROCESS_DATE_FORMAT.value) for x in range(0, (today-min_date).days + 1)]
            return_min_date = first_date
        return return_min_dates,return_dates
        
    