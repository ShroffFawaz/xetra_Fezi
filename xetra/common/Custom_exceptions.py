"""Custom Exceptions"""
class WrongFormatException(Exception):
    """
    Raised when the file format is not supported
    """
    pass
class WrongMetaFileException(Exception):
    """
    Raised when the metadata file is incorrect or corrupted
    """
    pass