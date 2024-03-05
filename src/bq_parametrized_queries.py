#
# NEL data query supposed to extract request-response pairs that contained both NEL & Report-To headers in the responses
#
QUERY_NEL_DATA = r"""
WITH final_extracted_table AS (
  WITH rt_extracted_values_table AS (
    WITH nel_values_extracted_table AS (
      WITH nel_extracted_table AS (
        WITH base_table AS (
          /* START base_table */
          -- DESC: Fetch all the base columns necessary
          SELECT
          requestid,
          firstReq,
          type,
          ext,
          url,
          status,
          LOWER(respOtherHeaders) resp_headers,

          FROM `%s`
          /* END base_table */
        )

        /* START nel_extracted_table */

        -- DESC:
        --  * Determine the number of 'total requests' and 'total unique domain requests'
        --  * Extract the NEL header value from the response
        SELECT
        requestid,
        firstReq,
        type,
        ext,
        url,
        status,
        resp_headers,

        /*REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,*/

        (SELECT COUNT(*) FROM base_table) AS total_crawled_resources,
        (SELECT COUNT(*) FROM base_table WHERE firstReq = true) AS total_crawled_domains,

        REGEXP_CONTAINS(resp_headers, r"(?:^|.*[\s,]+)(nel\s*[=]\s*)") AS contains_nel,
        REGEXP_EXTRACT(resp_headers, r"(?:^|.*[\s,]+)nel\s*[=]\s*({.*?})") AS nel_value,

        FROM base_table
        /* END nel_extracted_table */
      )

      /* START nel_values_extracted_table */
      -- DESC:
      --    * Extract NEL fields from the nel_value
      --    * Filter out responses with no NEL header
      SELECT
      requestid,
      firstReq,
      type,
      ext,
      url,
      status,
      resp_headers,

      -- url_domain,

      total_crawled_resources,
      total_crawled_domains,

      contains_nel,
      nel_value,

      -- Extract NEL values
      REGEXP_EXTRACT(nel_value, r".*max_age[\"\']\s*:\s*([0-9]+)")              AS nel_max_age,
      REGEXP_EXTRACT(nel_value, r".*failure[_]fraction[\"\']\s*:\s*([0-9\.]+)") AS nel_failure_fraction,
      REGEXP_EXTRACT(nel_value, r".*success[_]fraction[\"\']\s*:\s*([0-9\.]+)") AS nel_success_fraction,
      REGEXP_EXTRACT(nel_value, r".*include[_]subdomains[\"\']\s*:\s*(\w+)")    AS nel_include_subdomains,

      REGEXP_EXTRACT(nel_value, r".*report_to[\"\']\s*:\s*[\"\'](.+?)[\"\']")   AS nel_report_to,

      FROM nel_extracted_table

      -- Filter out the non-NEL responses
      WHERE contains_nel = true

      /* END nel_values_extracted_table */
    )

    /* START rt_extracted_values_table */
    -- DESC:
    --  * Store the total number of NEL headers found (before filtering out the ones incorrectly used)
    --  * Extract Report-To header value from the response

    SELECT
    requestid,
    firstReq,
    type,
    ext,
    url,
    status,

    -- url_domain,

    total_crawled_resources,
    total_crawled_domains,

    contains_nel,
    nel_value,

    nel_max_age,
    nel_failure_fraction,
    nel_success_fraction,
    nel_include_subdomains,
    nel_report_to,

    (SELECT COUNT(*) FROM nel_values_extracted_table) AS total_crawled_resources_with_nel,
    -- rt_value
    REGEXP_EXTRACT(resp_headers, CONCAT(r"report[-]to\s*?[=].*([{](?:(?:[^\{]*?endpoints.*?[\[][^\[]*?[\]][^\}]*?)|(?:[^\{]*?endpoints.*?[\{][^\{]*?[\}]))?[^\]\}]*?group[\'\"][:]\s*?[\'\"]", nel_report_to, r"(?:(?:[^\}]*?endpoints[^\}]*?[\[][^\[]*?[\]][^\{]*?)|(?:[^\}]*?endpoints.*?[\{][^\{]*?[\}]))?.*?[}])")) AS rt_value,

    FROM nel_values_extracted_table
    /* END rt_extracted_values_table */
  )

  /* START final_extracted_table */
  -- DESC: Extract Report-To fields from the rt_value
  SELECT
  requestid,
  firstReq,
  type,
  ext,
  url,
  status,

  -- url_domain,

  total_crawled_resources,
  total_crawled_domains,

  contains_nel,
  nel_value,

  nel_max_age,
  nel_failure_fraction,
  nel_success_fraction,
  nel_include_subdomains,
  nel_report_to,

  total_crawled_resources_with_nel,
  rt_value,

  REGEXP_EXTRACT(rt_value, r".*group[\"\']\s*:\s*[\"\'](.+?)[\"\']") AS rt_group,

  REGEXP_EXTRACT_ALL(rt_value, r"url[\"\']\s*:\s*[\"\']http[s]?:[\\]*?[\/][\\]*?[\/]([^\/]+?)[\\]*?[\/\"]")
    AS rt_collectors,

  FROM rt_extracted_values_table
  /* END final_extracted_table */
)

/* START TOP LEVEL QUERY */
-- DESC: Return data, for which the NEL and Report-To headers are set up correctly (filter out incorrect use)
SELECT
requestid,
firstReq,
type,
ext,
url,
status,

-- url_domain,

total_crawled_resources,
total_crawled_domains,

nel_max_age,
nel_failure_fraction,
nel_success_fraction,
nel_include_subdomains,
nel_report_to,

total_crawled_resources_with_nel,

rt_group,
rt_collectors,

FROM final_extracted_table

-- Filter out un-parseable data,
-- Data must have both report_to and NEL header
-- Filter out records containing either:
----     * non-json value,
----     * bad formatting (no value, missing brackets)
WHERE nel_report_to = rt_group and nel_report_to is not null and rt_group is not null;
/* END TOP LEVEL QUERY */
"""

# Part of the NEL_DATA query that:
#    fetches the NEL http header value,
#    but does not filter the data by 'WHERE contains_nel = true'
QUERY_NEL_VALUE_WITHOUT_FILTERING = r"""
        WITH base_table AS (
          /* START base_table */
          -- DESC: Fetch all the base columns necessary
          SELECT
          requestid,
          firstReq,
          type,
          ext,
          url,
          status,
          LOWER(respOtherHeaders) resp_headers,

          FROM `%s`
          /* END base_table */
        )

        /* START nel_extracted_table */

        -- DESC: 
        --  * Determine the number of 'total requests' and 'total unique domain requests' 
        --  * Extract the NEL header value from the response
        SELECT
        requestid,
        firstReq,
        type,
        ext,
        url,
        status,

        /*REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,*/

        (SELECT COUNT(*) FROM base_table) AS total_crawled_resources,
        (SELECT COUNT(*) FROM base_table WHERE firstReq = true) AS total_crawled_domains,

        REGEXP_CONTAINS(resp_headers, r"(?:^|.*[\s,]+)(nel\s*[=]\s*)") AS contains_nel,
        REGEXP_EXTRACT(resp_headers, r"(?:^|.*[\s,]+)nel\s*[=]\s*({.*?})") AS nel_value,

        FROM base_table
        /* END nel_extracted_table */
"""
