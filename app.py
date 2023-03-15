"""Running the Xetra ETL application"""
import logging
import logging.config

import yaml
from yaml.loader import SafeLoader

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

    print(s3_bucket_src.list_folder_content())

    logger.info("Connection establised")


if __name__ == "__main__":
    main()
