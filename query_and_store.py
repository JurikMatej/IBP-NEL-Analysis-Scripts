#!/usr/bin/env python3
"""
author:         Matej Jurík <xjurik12@stud.fit.vutbr.cz>

description:    Query the Google Cloud Platform's BigQuery for HTTP Archive datasets containing summary of requests
                and their responses generated by crawling the internet.
                The specific queries being run by this script extract all request & response data where
                the response returned with a NEL and Report-To headers.

                This script acts as the updated version of the former script created by Kamil Jeřábek
                (original repo: from https://github.com/kjerabek/nel-http-archive-analysis).
                The necessity of this script comes from the need to transfer a large volume of data from BigQuery.
                The former script somehow managed to query the HTTP Archive datasets via the base interface
                (google.cloud.bigquery API) without reaching its limits for the maximum size of each query results.
                Here, the Google Cloud Storage API (google.cloud.storage) is used to retrieve large volume of data.
                Inner works:
                    1. Query BigQuery for NEL Data
                    2. Store the data in a temporary table managed automatically by this script (see config section)
                    3. Export that temporary table to Google Cloud Storage bucket
                    (creates many small Parquet files --- blobs --- compressed using SNAPPY).
                    4. Download all blobs from the Google Cloud Storage bucket
                    5. Make a single Parquet file from all those blobs
                    6. Persist the file locally
                Set-up:
                    1. Create a Google Cloud project
                    2. Create Service Account with ADMIN privileges for GC BigQuery and GC Storage
                    3. Download a .json key for that service account to use in this script (see config section)
                    4. Create a Google Cloud Storage bucket and specify its name into this script's config
                    5. Create a "download_config.json" file that contains an array of objects describing the tables to
                    download and the names of the resulting file like so:
                        [
                            {
                                "input_desktop": [
                                  "httparchive.summary_requests.2018_09_01_desktop",
                                  "httparchive.summary_requests.2018_09_15_desktop"
                                ],

                                "input_mobile": [
                                  "httparchive.summary_requests.2018_09_01_mobile",
                                  "httparchive.summary_requests.2018_09_15_mobile"
                                ],

                                "processed_output": "nel_data_2018_09",

                                "public_suffix_list": ""
                            },
                            {
                            ...
                            },
                            ...
                        ]
                    Where each object in the global array represents an entry to download = monthly NEL data.
                    The "public_suffix_list" field is optional (TODO still to be defined).
                    Also, the name of the config file is configurable within the config of this script.

purpose:        Get raw HTTP Archive data from the Google Cloud and store it as an Apache Parquet file in order
                for it to be available locally for further processing and analysis.
                (IMPORTANT: do not modify the downloaded data itself - additional queries cost money)
"""


import json
import logging
import os
import pathlib
import sys
import time
from typing import List

import google.api_core.exceptions
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

from src.bq_parametrized_queries import \
    QUERY_NEL_DATA_2_DESKTOP_2_MOBILE, \
    QUERY_NEL_DATA_2_DESKTOP_1_MOBILE, \
    QUERY_NEL_DATA_1_DESKTOP_1_MOBILE, \
    QUERY_NEL_DATA_1_DESKTOP


# LOGGING
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s:%(levelname)s\t- %(message)s')
logger = logging.getLogger(__name__)


# Global constants
ONE_DAY_IN_MS = 1000 * 60 * 60 * 24


###############################
# CONFIGURE THESE BEFORE USE: #
###############################
GC_PROJECT_NAME = 'nel-analysis'                # Google Cloud Project Name
GC_BQ_DATASET_NAME = 'httparchive_fetch_temp'   # Google Cloud BigQuery Dataset - to hold a temporary table for the data
GC_BQ_TEMP_TABLE_NAME = 'nel_data'              # Google Cloud BigQuery Table - that temporary table for the data

GC_TABLE_LOCATION = "US"  # This should be good AS IS

# IMPORTANT:
# Use credentials with ADMIN (read, write, manage structure) privileges for Google Cloud BigQuery & Google Cloud Storage
GC_PATH_TO_CREDENTIALS_FILE = "gcp-secrets/nel-analysis-f1c127130c7f-nel-analysis-admin.json"

# Google Cloud Storage bucket name
DATA_EXPORT_BUCKET_NAME = "downloadable-nel-analysis-data"

# Download directory structure
DOWNLOAD_OUTPUT_DIR_PATH = "httparchive_data_raw"
DOWNLOAD_TEMP_BLOBS_DIR_PATH = f"{DOWNLOAD_OUTPUT_DIR_PATH}/blobs"

# Download config
DOWNLOAD_CONFIG_PATH = "download_config.json"


def prepare_nel_data_table():
    """
    Prepare required infrastructure inside BigQuery. Create a temporary dataset and a temporary table inside of it.
    This temporary table will then hold the data from the sequentially ran queries to get NEL data from BigQuery.
    """
    logger.info("##### Preparing temporary infrastructure for NEL analysis data")

    # Build a BigQuery infrastructure administration client instance
    client = _bq_infrastructure_administration_client()

    # Prepare infrastructure for working NEL data
    _create_temp_dataset(client)
    _create_temp_table(client)


def _bq_infrastructure_administration_client() -> bigquery.Client:
    """
    Create a simple BigQuery client able to handle infrastructure administration.
    Note that the client needs valid credentials with just enough privileges to create datasets,
    tables and execute query jobs.

    :return: BigQuery client meant to handle infrastructure administration
    """
    credentials = service_account.Credentials.from_service_account_file(
        GC_PATH_TO_CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(credentials=credentials, project=GC_PROJECT_NAME)

    return client


def _create_temp_dataset(client):
    """
    Creates a BigQuery dataset with a pre-configured name (see global config variables)
    """
    dataset_id = f"{client.project}.{GC_BQ_DATASET_NAME}"
    dataset = bigquery.Dataset(dataset_id)

    dataset.location = "US"  # Set specifically to US, because the HTTP Archive data are also located in the US
    dataset.description = "Dataset for NEL Analysis data"
    dataset.default_table_expiration_ms = ONE_DAY_IN_MS

    try:
        dataset = client.create_dataset(dataset, timeout=30)
        logger.info(f"Created dataset '{client.project}.{dataset.dataset_id}'")

    except google.api_core.exceptions.Conflict:
        logger.info(f"Dataset '{client.project}.{dataset.dataset_id}' already exists. Proceeding to use it")


def _create_temp_table(client: bigquery.Client):
    """
    Create a temporary BigQuery table for storing query results of data for the NEL analysis.
    If the temporary table already exists, delete all it's contents before using it.

    :param client: Basic Google Cloud BigQuery API client with credentials and project ID
                   already provided
    """
    table_id = f"{GC_PROJECT_NAME}.{GC_BQ_DATASET_NAME}.{GC_BQ_TEMP_TABLE_NAME}"

    nel_data_schema = [
        bigquery.SchemaField("requestId", "INTEGER"),
        bigquery.SchemaField("firstReq", "BOOLEAN"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("ext", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("url_etld", "STRING"),
        bigquery.SchemaField("status", "INTEGER"),
        bigquery.SchemaField("total_crawled_resources", "INTEGER"),
        bigquery.SchemaField("total_crawled_domains", "INTEGER"),
        bigquery.SchemaField("nel_max_age", "STRING"),
        bigquery.SchemaField("nel_failure_fraction", "STRING"),
        bigquery.SchemaField("nel_success_fraction", "STRING"),
        bigquery.SchemaField("nel_include_subdomains", "STRING"),
        bigquery.SchemaField("nel_report_to", "STRING"),
        bigquery.SchemaField("total_crawled_resources_with_nel", "INTEGER"),
        bigquery.SchemaField("rt_group", "STRING"),
        bigquery.SchemaField("rt_collectors", "STRING", mode="REPEATED"),
    ]

    table = bigquery.Table(table_id, schema=nel_data_schema)

    try:
        table = client.create_table(table)  # Make an API request.
        logger.info(f"Created temporary table '{table.project}.{table.dataset_id}.{table.table_id}'")

    except google.api_core.exceptions.Conflict:
        logger.info(f"Temporary table '{table.project}.{table.dataset_id}.{table.table_id}' already exists")

        # NOTE: billing needs to be enabled for the project in order for the DELETE OP to work
        delete_table_contents_query = f"DELETE FROM `{table_id}` WHERE 1=1;"
        client.query_and_wait(delete_table_contents_query)

        # No problem here, still using a warning to be careful when handling large data
        # because the temp table contains billed data (when deleted during debugging, it costs more money to reproduce)
        logger.warning("Temporary table contents deleted. Proceeding to use it")


def select_query_by_table_structure(desktop_table_list: List[str], mobile_table_list: List[str]) -> str:
    """
    Determine the query to be used based on how the monthly data tables are split.
    Currently, this method supports any kind of data table split for the time period from 2018/09 to 2024/01:
        * 2 desktop tables & 2 mobile tables
        * 2 desktop tables & 1 mobile table
        * 1 desktop table & 1 mobile table
        * 1 desktop table

    :param desktop_table_list: List of desktop table names to query for
    :param mobile_table_list: List of mobile table names to query for
    :return: Suitable query to fetch all tables provided in the parameters
    """
    if len(desktop_table_list) == 2 and len(mobile_table_list) == 2:
        return QUERY_NEL_DATA_2_DESKTOP_2_MOBILE % (
            desktop_table_list[1],
            desktop_table_list[1],
            desktop_table_list[0],
            desktop_table_list[0],
            mobile_table_list[1],
            mobile_table_list[1],
            mobile_table_list[0],
            mobile_table_list[0],
        )
    elif len(desktop_table_list) == 2 and len(mobile_table_list) == 1:
        return QUERY_NEL_DATA_2_DESKTOP_1_MOBILE % (
            desktop_table_list[1],
            desktop_table_list[1],
            desktop_table_list[0],
            desktop_table_list[0],
            mobile_table_list[0],
            mobile_table_list[0],
        )
    elif len(desktop_table_list) == 1 and len(mobile_table_list) == 1:
        return QUERY_NEL_DATA_1_DESKTOP_1_MOBILE % (
            desktop_table_list[0],
            desktop_table_list[0],
            mobile_table_list[0],
            mobile_table_list[0],
        )
    elif len(desktop_table_list) == 1 and len(mobile_table_list) == 0:
        return QUERY_NEL_DATA_1_DESKTOP % (
            desktop_table_list[0],
            desktop_table_list[0],
        )
    else:
        raise NotImplementedError("Other type of table fragmentation should not occur during the analyzed time period")


def populate_temp_table_with_query_results(client: bigquery.Client, query_string: str, output_filename: str):
    """
    Populate the temporary table managed by this script with data fetched by using the query string
    provided as an argument.

    :param client: Basic Google Cloud BigQuery API client with credentials and project ID
                   already provided
    :param query_string: Query to use for table population
    :param output_filename: Name of the output file (here only for logging info)
    """
    logger.info(f"##### Populating data for: {output_filename.upper()}")

    target_table_id = f"{GC_PROJECT_NAME}.{GC_BQ_DATASET_NAME}.{GC_BQ_TEMP_TABLE_NAME}"
    job_config = bigquery.QueryJobConfig(destination=target_table_id)

    client.query_and_wait(query_string, job_config=job_config)

    logger.info(f"The temporary table {target_table_id} was loaded successfully with {output_filename}")


def export_temp_table_to_storage_bucket_blobs(blob_name_prefix: str):
    """
    Export the temporary table managed by this script into the Google Cloud Storage bucket with name configured
    in this script's config.
    The data will be stored there as multiple Parquet blobs compressed with SNAPPY.

    :param blob_name_prefix: Name to be used as a prefix for the multiple blobs to be created.
                             Example:
                                * blob_name_prefix-00000001.parquet.snappy
                                * blob_name_prefix-00000002.parquet.snappy
                                ...
                                * blob_name_prefix-0000000N.parquet.snappy
    """
    logger.info("##### Extracting NEL data")
    client = _bq_infrastructure_administration_client()

    destination_uri = "gs://{}/{}".format(DATA_EXPORT_BUCKET_NAME,
                                          f"{blob_name_prefix}-*.parquet.snappy")
    dataset_ref = bigquery.DatasetReference(GC_PROJECT_NAME, GC_BQ_DATASET_NAME)
    table_ref = dataset_ref.table(GC_BQ_TEMP_TABLE_NAME)

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.PARQUET
    job_config.compression = bigquery.Compression.SNAPPY

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location=GC_TABLE_LOCATION,
        job_config=job_config,
    )

    extract_job.result()  # Waits for job to complete.

    logger.info("NEL data exported successfully and is ready to be downloaded")


def download_blobs_from_storage_bucket(storage_client: google.cloud.storage.Client, blob_name_prefix: str):
    """
    Downloads all blobs from the Google Cloud Storage bucket to local disk space.

    :param storage_client: Google Cloud Storage API client able to download blobs from a bucket
    :param blob_name_prefix: Name prefix for the blobs to download from the bucket
    """
    logger.info("##### Downloading exported NEL data")

    bucket = storage_client.get_bucket(DATA_EXPORT_BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=blob_name_prefix)

    dl_time = time.time()

    for blob in blobs:
        destination_uri = '{}/{}'.format(DOWNLOAD_TEMP_BLOBS_DIR_PATH, blob.name)
        blob.download_to_filename(destination_uri)

    logger.info("Exported NEL data downloaded successfully")
    logger.info(f"Download time: {time.time() - dl_time}")


def merge_downloaded_blobs_into_single_file(result_data_file_name: str):
    """
    Merge all downloaded NEL data blobs into a single .parquet file

    :param result_data_file_name: The name of the resulting single .parquet file
    """
    logger.info("##### Gathering downloaded NEL data into a single file")

    gather_time = time.time()

    blob_dir = pathlib.Path(DOWNLOAD_TEMP_BLOBS_DIR_PATH)
    files = list(blob_dir.glob('*.parquet.snappy'))

    if len(files) < 1:
        logger.warning("Download completed with 0 files downloaded, "
                       "aborting gathering downloaded NEL data into a single file")
        return

    schema = pq.ParquetFile(files[0]).schema_arrow

    result_path = f"{DOWNLOAD_OUTPUT_DIR_PATH}/{result_data_file_name}.parquet"
    with pq.ParquetWriter(result_path, schema=schema) as writer:
        for parquet_file in files:
            # Gather each standalone blob into a single file
            writer.write_table(pq.read_table(parquet_file, schema=schema))
            # Remove the blob after, so it does not get gathered to subsequent data tables
            parquet_file.unlink()

    print()
    logger.info(f"Result filesize: {os.path.getsize(result_path) / 2 ** 30} GB")
    logger.info(f"Gather time: {time.time() - gather_time}")


def _gc_storage_client() -> google.cloud.storage.Client:
    """
    Create an instance of the Google Cloud Storage client able to download blobs from buckets

    :return: The client instance
    """
    return storage.Client.from_service_account_json(GC_PATH_TO_CREDENTIALS_FILE)


def clean_storage_bucket(storage_client: google.cloud.storage.Client, blob_name_prefix: str):
    """
    Clean all blobs from the used Google Cloud Storage bucket

    :param storage_client: Instance of the Google Cloud Storage client
    :param blob_name_prefix: Name prefix of the blobs to delete
    """
    logger.info("##### Deleting exported NEL data")

    bucket = storage_client.get_bucket(DATA_EXPORT_BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=blob_name_prefix)

    for blob in blobs:
        blob.delete()

    logger.info("Exported NEL data deleted successfully")


def clean_temp_table(client: google.cloud.bigquery.Client):
    """
    Clean the temporary table - delete all rows from it

    :param client: Basic Google Cloud BigQuery API client with credentials and project ID
                   already provided
    """
    logger.info("##### Deleting temporary NEL data table contents")

    table_id = f"{GC_PROJECT_NAME}.{GC_BQ_DATASET_NAME}.{GC_BQ_TEMP_TABLE_NAME}"
    delete_table_contents_query = f"DELETE FROM `{table_id}` WHERE 1=1;"

    client.query_and_wait(delete_table_contents_query)

    logger.info("Temporary table contents deleted successfully")


def main():
    # Prepare BigQuery infrastructure for querying data
    query_client = _bq_infrastructure_administration_client()
    storage_client = _gc_storage_client()

    prepare_nel_data_table()
    print()

    # Prepare download infrastructure on this device
    pathlib.Path(DOWNLOAD_TEMP_BLOBS_DIR_PATH).mkdir(parents=True, exist_ok=True)

    # Run query & store
    with open(DOWNLOAD_CONFIG_PATH, 'r') as config_file:
        download_conf = json.loads(config_file.read())

        for item in download_conf:
            desktop_table_list = item.get('input_desktop', [])
            mobile_table_list = item.get('input_mobile', [])

            # Ensure the download config entry contains a "processed_output" output filename value
            output_filename = item.get('processed_output', None)
            if output_filename is None:
                logger.error(f"Tables with Desktop_1 {desktop_table_list[0]} - no output filename specified")
                continue

            # Skip this download entry if file with this entry's output filename already exists among downloaded files
            file_to_download_path = pathlib.Path(f"{DOWNLOAD_OUTPUT_DIR_PATH}/{output_filename}.parquet")
            if file_to_download_path.is_file():
                logger.info(f"Table {output_filename} already among downloaded files")
                continue

            # Query & Store a download entry...
            query = select_query_by_table_structure(desktop_table_list, mobile_table_list)
            print()

            populate_temp_table_with_query_results(query_client, query, output_filename)
            print()

            export_temp_table_to_storage_bucket_blobs(output_filename)
            print()

            download_blobs_from_storage_bucket(storage_client, output_filename)
            print()

            merge_downloaded_blobs_into_single_file(output_filename)
            print()

            clean_storage_bucket(storage_client, output_filename)
            print()

            clean_temp_table(query_client)
            print()


if '__main__' == __name__:
    main()
