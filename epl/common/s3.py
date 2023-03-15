"""Connector and methods accessing S3"""


import logging
import os

import boto3

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

    def list_files_in_prefix(self, prefix: str):
        """
        listing all files with a prefix on the S3 bucket

        :param prefix: prefix on the S3 bucket that should be filtered with

        returns:
          files: list of all the file names containing the prefix in the key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def list_folder_content(self):
        """
        listing busket root folder

        returns:
          files: list of all the folder names
        """
        files = [
            obj.key
            for obj in self._bucket.objects.filter(Prefix="")
            if S3FileTypes.CSV.value not in obj.key
        ]
        return files
