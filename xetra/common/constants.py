"""
File to store contants
"""

from enum import Enum

class S3FileType(Enum):
    """
    supported file types for s3bucketconnetor
    """
    CSV='csv'
    PARQUET='parquet'


class MetaprocessFormat(Enum):
    """
    Formation for Metaprocess class
    """

    META_DATE_FORMAT='%y-%m-%d'
    META_PROCESS_DATE_FORMAT='%y-%m-%d %H:%M:%S'
    META_SOURCE_DATE_COL='source_date'
    META_PROCESS_COL='datetime_of_processing'
    META_FILE_FORMAT='csv'