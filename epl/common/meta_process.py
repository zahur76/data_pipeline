"""
Class to dermine which folder needs to be processed
"""

import datetime
import logging

from epl.common.constants import S3FileTypes
from epl.common.s3 import S3BucketConnector


class MetaProcess:
    """
    Takes an input date and returns list of folders requiring processing
    :param src_dataframe: source dataframe object
    """

    def __init__(self, s3_bucket_src: S3BucketConnector):
        self.s3_bucket_src = s3_bucket_src
        self._logger = logging.getLogger(__name__)

    def execution_list(self, tgr_date=datetime.datetime.now().strftime("%Y-%m-%d")):
        """
        Returns a list of dates requiring processing
        :param date, optional: selected date to be processed
        """

        all_folders = self.s3_bucket_src.list_folders()

        execution_list = [
            day
            for day in all_folders
            if datetime.datetime.strptime(day, "%Y-%m-%d")
            <= datetime.datetime.strptime(tgr_date, "%Y-%m-%d")
        ]

        return execution_list
