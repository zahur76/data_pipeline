"""Running the Xetra ETL application"""
import datetime
import logging
import logging.config

import yaml
from yaml.loader import SafeLoader

from epl.common.meta_process import MetaProcess
from epl.common.s3 import S3BucketConnector
from epl.transfomers.epl_transformer import ETLExecutor


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

    logger.debug("S3 Bucket Connection establised")

    # create execution list object
    execution = MetaProcess(s3_bucket_src)
    # Obtain list of folders requiring processing
    execution_dates = execution.execution_list()

    # apply transformer operation to source and target buckets for data
    # before and including date
    date_list = []
    for date in execution_dates:
        ETLExecutor(s3_bucket_src, s3_bucket_trg).transform(date)
        logger.debug(f"{date} Processed")
        date_list.append([date, datetime.datetime.now().strftime("%Y-%m-%d")])
    # create meta file for stored data
    s3_bucket_src.save_meta_file_to_s3(date_list)

    logger.info(f"Job Completed-{datetime.datetime.now().strftime('%Y-%m-%d-%h%m')}")


if __name__ == "__main__":
    main()
