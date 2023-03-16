"""Running the Xetra ETL application"""
import logging
import logging.config

import yaml
from yaml.loader import SafeLoader

from epl.common.custom_exceptions import CombiningError
from epl.common.s3 import S3BucketConnector


def main():
    """
    entry point to run the xetra ETL job
    """
    # Open and parsing YAML file
    with open("config/epl_config.yml") as f:
        config = yaml.load(f, Loader=SafeLoader)

    # configure logging
    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    # reading s3 configuration
    s3_config = config["s3"]

    # creating the S3BucketConnector class instances for source and target

    s3_bucket_src = S3BucketConnector(
        access_key=s3_config["access_key"],
        secret_key=s3_config["secret_key"],
        bucket=s3_config["src_bucket"],
    )

    s3_bucket_trg = S3BucketConnector(
        access_key=s3_config["access_key"],
        secret_key=s3_config["secret_key"],
        bucket=s3_config["trg_bucket"],
    )

    logger.debug("Connection establised")

    key_list = s3_bucket_src.list_files_in_prefix("2023-03-01")

    df = s3_bucket_src.read_csv_list_combine_convert_to_df(key_list)

    logger.debug("Dataframe Extracted and combined")

    s3_bucket_trg.write_df_to_s3(df, key='data/processed-data', file_format='csv')

    logger.info("Job Completed")

if __name__ == "__main__":
    main()
