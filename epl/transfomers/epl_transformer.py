""" File Transfomer """
import datetime
import logging

from epl.common.s3 import S3BucketConnector


class ETLExecutor:
    """
    Reads input dataframe, manipulates and then returns updated dataframe
    :param src_dataframe: source dataframe connection
    :param tgr_dataframe: target dataframe connection
    """

    def __init__(
        self, src_dataframe: S3BucketConnector, tgr_dataframe: S3BucketConnector
    ):
        self.src_dataframe = src_dataframe
        self.tgr_dataframe = tgr_dataframe
        self._logger = logging.getLogger(__name__)

    def transform(self, day):
        """
        Peforms the ETL operation on input file by date and
        saves to output bucket
        :param day: chosen date defaults to today
        """

        key_list = self.src_dataframe.list_files_in_prefix(day)
        self._logger.debug("Load Completed")
        df = self.src_dataframe.read_csv_list_combine_convert_to_df(key_list)
        self._logger.debug("Combining Completed")
        _df = self.transformer(df)
        self._logger.debug("Transforming complete")
        if not df.empty:
            self.tgr_dataframe.write_df_to_s3(
                _df,
                key=f"data/processed-data-{day}",
                file_format="csv",
            )
            return None

    def transformer(self, df):
        """
        Transforms data by adding additional coloumn
        :param df: input dataframe

        returns: dataframe with additional column
        """

        df["Is processed"] = True

        return df
