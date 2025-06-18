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
    def retrun_date_list():
        pass
    