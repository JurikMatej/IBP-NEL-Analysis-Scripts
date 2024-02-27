#!/usr/bin/env python3

import os
from dotenv import load_dotenv

from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

GCP_BQ_CREDENTIALS_FILE_PATH = os.environ['GCP_BQ_CREDENTIALS']
GCP_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

credentials = service_account.Credentials.from_service_account_file(
    GCP_BQ_CREDENTIALS_FILE_PATH,
    scopes=GCP_SCOPES
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)

mobile_summary_tables = [
    # 'httparchive.summary_requests.2018_02_01_mobile',
    # 'httparchive.summary_requests.2019_02_01_mobile',
    # 'httparchive.summary_requests.2020_02_01_mobile',
    # 'httparchive.summary_requests.2021_02_01_mobile',
    # 'httparchive.summary_requests.2022_02_01_mobile',
    # 'httparchive.summary_requests.2023_02_01_mobile',
]

desktop_summary_tables = [
    # 'httparchive.summary_requests.2018_02_01_desktop',
    # 'httparchive.summary_requests.2019_02_01_desktop',
    # 'httparchive.summary_requests.2020_02_01_desktop',
    # 'httparchive.summary_requests.2021_02_01_desktop',
    'httparchive.summary_requests.2021_01_01_desktop',
    # 'httparchive.summary_requests.2022_02_01_desktop',
]


query_string = r'''
WITH final_extracted_table AS (
  WITH rt_extracted_values_table AS (
    WITH nel_values_extracted_table AS (
      WITH nel_extracted_table AS (
        WITH joined_table AS (
          WITH filtered_table AS (
            SELECT
            MIN(requestid) min_req_id,
            REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
            FROM `%s`
            GROUP BY url_domain
          )
          SELECT
          requestid,
          LOWER(respOtherHeaders) resp_headers,
          status,
          url,
          type,
          ext,
          firstReq,
          FROM filtered_table
          INNER JOIN `%s` ON filtered_table.min_req_id = requestid
        )

        SELECT
        requestid,
        type,
        ext,
        firstReq,
        status,
        url,
        resp_headers,

        (SELECT COUNT(*) FROM joined_table) AS unique_domain_count_before_filtration,
        (SELECT COUNT(*) FROM joined_table WHERE firstReq = true) AS unique_domain_firstreq_count_before_filtration,
        REGEXP_CONTAINS(resp_headers, r"(?:^|.*[\s,]+)(nel\s*[=]\s*)") AS contains_nel,
        REGEXP_EXTRACT(resp_headers, r"(?:^|.*[\s,]+)nel\s*[=]\s*({.*?})") AS nel_value,

        FROM joined_table
      )
      SELECT
      requestid,
      type,
      ext,
      firstReq,
      status,
      url,
      resp_headers,
      unique_domain_count_before_filtration,
      unique_domain_firstreq_count_before_filtration,
      contains_nel,

      nel_value,

      # extract nel values
      REGEXP_EXTRACT(nel_value, r".*max_age[\"\']\s*:\s*([0-9]+)") AS nel_max_age,
      REGEXP_EXTRACT(nel_value, r".*failure[_]fraction[\"\']\s*:\s*([0-9\.]+)") AS nel_failure_fraction,
      REGEXP_EXTRACT(nel_value, r".*success[_]fraction[\"\']\s*:\s*([0-9\.]+)") AS nel_success_fraction,
      REGEXP_EXTRACT(nel_value, r".*include[_]subdomains[\"\']\s*:\s*(\w+)") AS nel_include_subdomains,

      REGEXP_EXTRACT(nel_value, r".*report_to[\"\']\s*:\s*[\"\'](.+?)[\"\']") AS nel_report_to_group,

      FROM nel_extracted_table
      WHERE contains_nel = true
    )

    SELECT
    requestid,
    type,
    ext,
    firstReq,
    status,
    url,
    unique_domain_count_before_filtration,
    unique_domain_firstreq_count_before_filtration,
    contains_nel,
    nel_max_age,
    nel_failure_fraction,
    nel_success_fraction,
    nel_include_subdomains,
    nel_report_to_group,
    resp_headers,

    (SELECT COUNT(*) FROM nel_values_extracted_table) AS nel_count_before_filtration,

    REGEXP_EXTRACT(resp_headers, CONCAT(r"report[-]to\s*?[=].*([{](?:(?:[^\{]*?endpoints.*?[\[][^\[]*?[\]][^\}]*?)|(?:[^\{]*?endpoints.*?[\{][^\{]*?[\}]))?[^\]\}]*?group[\'\"][:]\s*?[\'\"]", nel_report_to_group, r"(?:(?:[^\}]*?endpoints[^\}]*?[\[][^\[]*?[\]][^\{]*?)|(?:[^\}]*?endpoints.*?[\{][^\{]*?[\}]))?.*?[}])")) AS rt_value,

    FROM nel_values_extracted_table
  )
  SELECT
  requestid,
  type,
  ext,
  firstReq,
  status,
  url,
  unique_domain_count_before_filtration,
  unique_domain_firstreq_count_before_filtration,
  contains_nel,
  nel_max_age,
  nel_failure_fraction,
  nel_success_fraction,
  nel_include_subdomains,
  nel_report_to_group,
  rt_value,
  nel_count_before_filtration,

  REGEXP_EXTRACT(rt_value, r".*group[\"\']\s*:\s*[\"\'](.+?)[\"\']") AS rt_group,

  REGEXP_EXTRACT_ALL(rt_value, r"url[\"\']\s*:\s*[\"\']http[s]?:[\\]*?[\/][\\]*?[\/]([^\/]+?)[\\]*?[\/\"]") AS rt_endpoints,
  REGEXP_EXTRACT(rt_value, r"url[\"\']\s*:\s*[\"\']http[s]?:[\\]*?[\/][\\]*?[\/]([^\/]+?)[\\]*?[\/\"]") AS rt_url,
  REGEXP_EXTRACT(rt_value, r"url[\"\']\s*:\s*(?:[\"\']http[s]?:[\\]*?[\/][\\]*?[\/].*?([^\.]+?[.][^\.]+?)[\\]*?[\/\"])") AS rt_url_sld,

  FROM rt_extracted_values_table
)

SELECT
requestid,
type,
ext,
firstReq,
status,
url,
unique_domain_count_before_filtration,
unique_domain_firstreq_count_before_filtration,
contains_nel,
nel_max_age,
nel_failure_fraction,
nel_success_fraction,
nel_include_subdomains,
nel_report_to_group,
nel_count_before_filtration,
rt_group,
rt_endpoints,
rt_url,
rt_url_sld,

FROM final_extracted_table
# filter data to only those that we could parse, and that has both report_to and group matching
# by analysis, the filtered out records contains either not json value, bad formating such as \" for json quotes, or are maybe improperly parsed by previous processing by table creators (no value, missing brackets)
WHERE nel_report_to_group = rt_group and nel_report_to_group is not null and rt_group is not null;

'''


def processing_bytes_estimation(table_list, query_string):
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    sum_mb = 0

    for table_name in table_list:
        query_job = client.query(
            (
                query_string % (table_name, table_name)
            ),
            job_config=job_config,
        )
        processed_mb = query_job.total_bytes_processed/1024/1024
        print("This query '{}' will process {} MB, {} GB.".format(table_name, processed_mb, processed_mb/1024))
        sum_mb += processed_mb

    print()
    print("Total will process {} MB, {} GB, {} TB".format(sum_mb, sum_mb/1024, sum_mb/1024/1024))


def run_queries_store_data(results_dir, table_list, query_string) -> list:
    job_config = bigquery.QueryJobConfig()
    job_list = []

    for table_name in table_list:
        query_job = client.query(
            (
                query_string % (table_name, table_name)
            ),
            job_config=job_config,
        )

        dst_tmp_table_dict = query_job.__dict__['_properties']['configuration']['query']['destinationTable']
        dst_tmp_table_name = f'{dst_tmp_table_dict["projectId"]}.{dst_tmp_table_dict["datasetId"]}.{dst_tmp_table_dict["tableId"]}'

        tmp_df = query_job.to_dataframe()

        tmp_df.to_parquet(f"{results_dir}/{table_name}.{dst_tmp_table_name}.parquet")
        job_list.append(query_job)

    return job_list

run_params = [
    ('results_mobile_all_feb', mobile_summary_tables),
    ('results_desktop_all_feb', desktop_summary_tables)
    ]

for params in run_params:
    processing_bytes_estimation(params[1], query_string)

    job_list = run_queries_store_data(params[0], params[1], query_string)