""" File Transfomer """
import datetime
import logging

from epl.common.s3 import S3BucketConnector


class ETLExecutor:
    """
    Reads input dataframe, manipulates and then returns updated dataframe
    :param src_dataframe: source dataframe object
    :param tgr_dataframe: target dataframe object
    """

    def __init__(
        self, src_dataframe: S3BucketConnector, tgr_dataframe: S3BucketConnector
    ):
        self.src_dataframe = src_dataframe
        self.tgr_dataframe = tgr_dataframe
        self._logger = logging.getLogger(__name__)

    def transform(self):
        """
        Peforms the ETL operation on input file by date and
        saves to output bucket
        """
        key_list = self.src_dataframe.list_files_in_prefix("2023-03-01")
        self._logger.info("Load Completed")
        df = self.src_dataframe.read_csv_list_combine_convert_to_df(key_list)
        self._logger.info("Combine Completed")
        self.tgr_dataframe.write_df_to_s3(
            df, key=f"data/processed-data-{datetime.datetime.now()}", file_format="csv"
        )
        return None
