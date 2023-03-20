""" Connector and methods to access S3 resource """


import logging
import os
from io import BytesIO, StringIO

import boto3
import pandas as pd

from epl.common.constants import S3FileTypes


class S3BucketConnector:
    """
    Class for interacting with S3 Buckets
    """

    def __init__(self, access_key: str, secret_key: str, bucket: str):
        """
        Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3
        :param bucket: S3 bucket name
        """

        self._logger = logging.getLogger(__name__)
        self.session = boto3.Session(
            aws_access_key_id=os.environ[access_key],
            aws_secret_access_key=os.environ[secret_key],
        )
        self._s3 = self.session.resource(service_name="s3")
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, tgr_date: str):
        """
        listing all files with a prefix on the S3 bucket with target date

        :param tgr date: will obtain prefix on the S3 bucket filtered with tgr date, format: yyyy-mm-dd

        returns:
          key: absolute path to files, list of all the file names containing the prefix in the key for that date
        """

        try:
            # return prefix/folder name for the target date
            prefix = [
                obj.key
                for obj in self._bucket.objects.filter(Prefix="")
                if S3FileTypes.CSV.value not in obj.key and tgr_date in obj.key
            ]
            # obtain files with above prefix/folder without root folder
            files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix[0])]

            return files[1:]
        except IndexError:
            self._logger.info("List is empty")
            files = None

    def list_folders(self):
        """
        List all folder in src bucket and check which folders require proccesing

        returns:
          files: list of all the folder names
        """

        # load meta data file and obtain list of folder updated
        load_meta_data = self._bucket.Object(key="processed_data.csv").get().get("Body")

        df = pd.read_csv(load_meta_data)

        processed_list = df["folder"].tolist()

        # returns list of folder dates requiring processing
        all_folders = [
            obj.key.replace("football-", "").replace("/", "")
            for obj in self._bucket.objects.filter(Prefix="")
            if S3FileTypes.CSV.value not in obj.key
            and obj.key.replace("football-", "").replace("/", "") not in processed_list
        ]

        return all_folders

    def read_csv_list_combine_convert_to_df(
        self, key_list: list, encoding: str = "utf-8", sep: str = ","
    ):
        """
        Read a list of keys from the S3 bucket folder and returns a dataframe

        :key_list: list of keys to combine
        :encoding: encoding of the data inside the csv file
        :sep: seperator of the csv file

        returns:
          data_frame: Pandas DataFrame containing the data of the csv files combined
        """

        df_list = [
            pd.read_csv(self._bucket.Object(key=obj).get().get("Body"))
            for obj in key_list
        ]

        if len(df_list) == 0:
            self._logger.info("Empty Folder")
            return pd.DataFrame()
        df2 = pd.concat(df_list, ignore_index=True)

        return df2

    def write_df_to_s3(self, data_frame: pd.DataFrame, key: str, file_format: str):
        """
        writing a Pandas DataFrame to S3
        supported formats: .csv, .parquet

        :data_frame: Pandas DataFrame that should be written
        :key: target key of the saved file
        :file_format: format of the saved file
        """

        key = f"{key}.{file_format}"

        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)

    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()

        :key: target key of the saved file
        """

        self._logger.info(f"Writing file to {self._bucket.name}/{key}")
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True

    def update_meta_file_to_s3(self, date_list: list):
        """
        Make csv file of processed folders and save
        :param: list of lists of processed dates
        """
        meta_data = self._bucket.Object(key="processed_data.csv").get().get("Body")

        df = pd.read_csv(meta_data)

        df2 = pd.DataFrame(date_list, columns=["folder", "Processed date"])

        updated_df = pd.concat([df, df2], ignore_index=True)

        out_buffer = StringIO()

        updated_df.to_csv(out_buffer, index=False)

        return self.__put_object(out_buffer, "processed_data.csv")
