"""Date Pipe Line Integration Test"""
import datetime
import unittest

import boto3
import yaml
from yaml.loader import SafeLoader

from epl.common.meta_process import MetaProcess
from epl.common.s3 import S3BucketConnector
from epl.transfomers.epl_transformer import ETLExecutor


class PipelineIntegrationTest(unittest.TestCase):
    """
    Integration Testing
    """

    def setUp(self):
        """
        Setting up the environment
        """

        # Open and parsing YAML file
        with open("config/epl_config.yml") as f:
            config = yaml.load(f, Loader=SafeLoader)

        self.s3_config = config["s3"]

        # Defining the class arguments
        # Defining the class arguments
        self.s3_access_key = "AWS_ACCESS_KEY_ID"
        self.s3_secret_key = "AWS_SECRET_ACCESS_KEY"
        self.s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
        self.s3_src_bucket_name = "data-pipline-int-input"
        self.s3_tgr_bucket_name = "data-pipeline-int-output"

        # Creating a bucket on s3
        self.s3 = boto3.resource("s3")
        self.src_bucket = self.s3.Bucket(self.s3_src_bucket_name)
        self.trg_bucket = self.s3.Bucket(self.s3_tgr_bucket_name)

        # set-up test data
        prefix_exp = "football-2023-03-18/"
        prefix_exp2 = "football-2023-03-19/"

        # Meta init
        val1 = "2023-03-18"
        val2 = "2023-03-18"
        csv_content1 = f"folder,Processed date\n{val1},{val2}"

        val3 = 1
        val4 = 2

        csv_content2 = f"Data1,Data2\n{val3},{val4}"

        self.src_bucket.put_object(Body=csv_content1, Key="processed_data.csv")
        self.src_bucket.put_object(Key=prefix_exp)
        self.src_bucket.put_object(Key=prefix_exp2)
        self.src_bucket.put_object(
            Body=csv_content2, Key="football-2023-03-19/data1.csv"
        )

        self.src_bucket.put_object(Key=prefix_exp)
        self.src_bucket.put_object(Key=prefix_exp2)
        self.src_bucket.put_object(
            Body=csv_content2, Key="football-2023-03-19/data2.csv"
        )

        # bucket connections to test methods
        self.s3_bucket_src = S3BucketConnector(
            access_key=self.s3_config["access_key"],
            secret_key=self.s3_config["secret_key"],
            bucket=self.s3_config["int_test_src_bucket"],
        )

        self.s3_bucket_trg = S3BucketConnector(
            access_key=self.s3_config["access_key"],
            secret_key=self.s3_config["secret_key"],
            bucket=self.s3_config["int_test_tgr_bucket"],
        )

    def tearDown(self):
        for key in self.src_bucket.objects.all():
            key.delete()
        for key in self.trg_bucket.objects.all():
            key.delete()

    def test_integration(self):
        execution = MetaProcess(self.s3_bucket_src)
        execution_dates = execution.execution_list()
        date_list = []
        for date in execution_dates:
            ETLExecutor(self.s3_bucket_src, self.s3_bucket_trg).transform(date)
            date_list.append([date, datetime.datetime.now().strftime("%Y-%m-%d")])
        # create meta file for stored data
        self.s3_bucket_src.update_meta_file_to_s3(date_list)

        bucket_obj = self.trg_bucket.objects.filter(Prefix="data/")

        obj = [obj for obj in bucket_obj]

        self.assertEqual(obj[0].key, "data/processed-data-2023-03-19.csv")


if __name__ == "__main__":
    unittest.main()
