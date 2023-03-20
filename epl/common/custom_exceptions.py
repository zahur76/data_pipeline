"""Custom Exceptions"""


class WrongFormatException(Exception):
    """
    WrongFormatException class

    Exception that can be raised when the format type
    given as parameter is not supported.
    """


class WrongMetaFileException(Exception):
    """
    WrongMetaFileException class

    Exception that can be raised when the meta file
    format is not correct.
    """


class CombiningError(Exception):
    """
    Empty query class

    Exception that can be raised when the return list is empty
    """

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message
