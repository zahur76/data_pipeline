"""TestS3BucketConnectorMethods"""
import os
import unittest

import boto3
import pandas as pd
from moto import mock_s3

from epl.common.s3 import S3BucketConnector


class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector class
    """

    def setUp(self):
        """
        Setting up the environment
        """
        # mocking s3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = "AWS_ACCESS_KEY_ID"
        self.s3_secret_key = "AWS_SECRET_ACCESS_KEY"
        self.s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
        self.s3_bucket_name = "test-bucket"
        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = "KEY1"
        os.environ[self.s3_secret_key] = "KEY2"
        # Creating a bucket on the mocked s3
        self.s3 = boto3.resource(service_name="s3", endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(
            Bucket=self.s3_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating a testing instance
        self.s3_bucket_conn = S3BucketConnector(
            self.s3_access_key, self.s3_secret_key, self.s3_bucket_name
        )

    def tearDown(self):
        """
        Executing after unittests
        """
        # mocking s3 connection stop
        self.mock_s3.stop()

    def test_list_files_in_prefix_ok(self):
        """
        Tests the list_files_in_prefix method for getting 2 file keys
        as list on the mocked s3 bucket
        """
        # Test Data
        prefix_exp = "prefix-2023-03-18/"
        key1_exp = f"{prefix_exp}test1.csv"
        key2_exp = f"{prefix_exp}test2.csv"
        # Test init
        csv_content = """col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Key=prefix_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)

        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix("2023-03-18")
        x = 2
        # Tests after method execution
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)
        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={
                "Objects": [{"Key": prefix_exp}, {"Key": key1_exp}, {"Key": key2_exp}]
            }
        )

    def test_list_files_in_prefix_not_exist(self):
        """
        Tests the list_files_in_prefix method for getting 2 file keys
        as list on the mocked s3 bucket
        """
        # Test Data
        prefix_exp = "prefix-2023-03-18/"
        key1_exp = f"{prefix_exp}test1.csv"
        key2_exp = f"{prefix_exp}test2.csv"
        # Test init
        csv_content = """col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Key=prefix_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)

        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix("2023-03-19")

        # Tests after method execution
        self.assertEqual(list_result, None)
        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={
                "Objects": [{"Key": prefix_exp}, {"Key": key1_exp}, {"Key": key2_exp}]
            }
        )

    def test_to_list_all_folders_requiring_processing(self):
        """
        Test folders against processed_data.csv file to see
        which folders have already been processed
        """
        # Test Folder Data
        prefix_exp = "football-2023-03-18/"
        prefix_exp2 = "football-2023-03-19/"
        processed_data = "processed_data.csv"

        # Test init
        val1 = "2023-03-18"
        val2 = "2023-03-17"

        csv_content = f"folder,Processed_date\n{val1},{val2}"

        self.s3_bucket.put_object(Key=prefix_exp)
        self.s3_bucket.put_object(Key=prefix_exp2)
        self.s3_bucket.put_object(Body=csv_content, Key=processed_data)

        # Method execution
        list_result = self.s3_bucket_conn.list_folders()

        # Tests after method execution
        self.assertEqual(len(list_result), 1)
        self.assertEqual((list_result), ["2023-03-19"])
        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={
                "Objects": [
                    {"Key": prefix_exp},
                    {"Key": prefix_exp2},
                    {"Key": processed_data},
                ]
            }
        )

    def test_to_read_in_csv_covert_to_df_and_combine(self):
        """
        Test read to csv method
        """
        # Test Folder Data
        prefix = "prefix-2023-03-18/"
        key1 = "prefix-2023-03-18/data1.csv"
        key2 = "prefix-2023-03-19/data2.csv"

        # Test init
        val1 = "2023-03-18"
        val2 = "2023-03-17"

        csv_content1 = f"col1,col2\n{val1},{val2}"
        csv_content2 = f"col1,col2\n{val1},{val2}"

        self.s3_bucket.put_object(Key=prefix)
        self.s3_bucket.put_object(Body=csv_content1, Key=key1)
        self.s3_bucket.put_object(Body=csv_content2, Key=key2)

        # Method execution
        list_result = self.s3_bucket_conn.read_csv_list_combine_convert_to_df(
            [key1, key2]
        )

        # Tests after method execution
        self.assertEqual(list_result.shape[0], 2)

        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": prefix}, {"Key": key1}, {"Key": key2}]}
        )


if __name__ == "__main__":
    unittest.main()
