# TODO check all documentation

QUERY_NEL_DATA_1_DESKTOP = r"""
WITH final_extracted_table AS (
  WITH rt_extracted_values_table AS (
    WITH nel_values_extracted_table AS (
      WITH nel_extracted_table AS (
        WITH base_table AS (     
          /* START base_table */
          -- DESC: Fetch all the base columns necessary from both Desktop and Mobile variants of monthly data
          
          WITH unique_desktop AS ( 
            -- Fetch only the latest resource crawl for a unique url (resource)
            SELECT
            MAX(requestid) last_request_for_that_resource,
            url AS unique_url
            FROM `%s`
            GROUP BY unique_url
            
          )
          
          -- Add all other required columns to the already fetched unique resource crawls
          SELECT
          requestid,
          firstReq,
          type,
          ext,
          url,
          status,
          LOWER(respOtherHeaders) resp_headers,
          
          FROM unique_desktop 
          INNER JOIN `%s` ON unique_desktop.last_request_for_that_resource = requestid
          
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
NET.PUBLIC_SUFFIX(url) as url_etld,
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

FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY url) unique_url_number
    FROM final_extracted_table
)

-- Filter out un-parseable data,
-- Data must have both report_to and NEL header
-- Filter out records containing either:
----     * non-json value,
----     * bad formatting (no value, missing brackets)
WHERE nel_report_to = rt_group 
      AND nel_report_to is not null 
      AND rt_group is not null
      AND unique_url_number = 1
/* END TOP LEVEL QUERY */
"""

QUERY_NEL_DATA_1_DESKTOP_1_MOBILE = r"""
WITH final_extracted_table AS (
  WITH rt_extracted_values_table AS (
    WITH nel_values_extracted_table AS (
      WITH nel_extracted_table AS (
        WITH merged_unique_url_table AS (
          WITH base_table AS (     
            /* START base_table */
            -- DESC: Fetch all the base columns necessary from both Desktop and Mobile variants of monthly data
            
            WITH unique_desktop AS ( 
              -- Fetch only the latest resource crawl for a unique url (resource)
              SELECT
              MAX(requestid) last_request_for_that_resource,
              url AS unique_url
              FROM `%s`
              GROUP BY unique_url
              
            )
            
            -- Add all other required columns to the already fetched unique resource crawls
            SELECT
            requestid,
            startedDateTime,
            firstReq,
            type,
            ext,
            url,
            status,
            LOWER(respOtherHeaders) resp_headers,

            FROM unique_desktop 
            INNER JOIN `%s` ON unique_desktop.last_request_for_that_resource = requestid


            UNION ALL ( -- Merge Desktop data with Mobile data (UNION DISTINC works only for one column UNION, using UNION ALL here to avoid filtering out by requestId)


              WITH unique_mobile AS (
                  -- Fetch only the latest resource crawl for a unique url (resource)
                  SELECT
                  MAX(requestid) last_request_for_that_resource,
                  url AS unique_url
                  FROM `%s`
                  GROUP BY unique_url
              )
              
              -- Add all other required columns to the already fetched unique resource crawls
              SELECT
              requestid,
              startedDateTime,
              firstReq,
              type,
              ext,
              url,
              status,
              LOWER(respOtherHeaders) resp_headers,
              
              FROM unique_mobile
              INNER JOIN `%s` ON unique_mobile.last_request_for_that_resource = requestid
            )

            /* END base_table */
          )

          /* START merged_unique_url_table */
          -- DESC: UNION DISTINCT between desktop and mobile data does not work in this particular scenario
          --       Use another groupby to eliminate the duplicate url requests

          SELECT 
          requestid,
          startedDateTime,
          firstReq,
          type,
          ext,
          url,
          status,
          resp_headers,

          FROM base_table 

          /* END merged_unique_url_table */
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

        (SELECT COUNT(*) FROM merged_unique_url_table) AS total_crawled_resources,
        (SELECT COUNT(*) FROM merged_unique_url_table WHERE firstReq = true) AS total_crawled_domains,

        REGEXP_CONTAINS(resp_headers, r"(?:^|.*[\s,]+)(nel\s*[=]\s*)") AS contains_nel,
        REGEXP_EXTRACT(resp_headers, r"(?:^|.*[\s,]+)nel\s*[=]\s*({.*?})") AS nel_value,

        FROM merged_unique_url_table
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
NET.PUBLIC_SUFFIX(url) as url_etld,
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

FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY url) unique_url_number
    FROM final_extracted_table
)

-- Filter out un-parseable data,
-- Data must have both report_to and NEL header
-- Filter out records containing either:
----     * non-json value,
----     * bad formatting (no value, missing brackets)
WHERE nel_report_to = rt_group 
      AND nel_report_to is not null 
      AND rt_group is not null
      AND unique_url_number = 1
/* END TOP LEVEL QUERY */
"""

QUERY_NEL_DATA_2_DESKTOP_1_MOBILE = r"""
WITH final_extracted_table AS (
  WITH rt_extracted_values_table AS (
    WITH nel_values_extracted_table AS (
      WITH nel_extracted_table AS (
        WITH merged_unique_url_table AS (
          WITH base_table AS (      
            /* START base_table */
            -- DESC: Fetch all the base columns necessary from both Desktop and Mobile variants of monthly data
            
            WITH unique_desktop_2 AS ( 
              -- Fetch only the latest resource crawl for a unique url (resource)
              SELECT
              MAX(requestid) last_request_for_that_resource,
              url AS unique_url
              FROM `%s`
              GROUP BY unique_url
              
            )
            
            -- Add all other required columns to the already fetched unique resource crawls
            SELECT
            requestid,
            startedDateTime,
            firstReq,
            type,
            ext,
            url,
            status,
            LOWER(respOtherHeaders) resp_headers,

            FROM unique_desktop_2 
            INNER JOIN `%s` ON unique_desktop_2.last_request_for_that_resource = requestid


            UNION ALL ( -- Merge Desktop 1 data with Desktop 2 data (UNION DISTINC works only for one column UNION, using UNION ALL here to avoid filtering out by requestId)


              WITH unique_desktop_1 AS (
                  -- Fetch only the latest resource crawl for a unique url (resource)
                  SELECT
                  MAX(requestid) last_request_for_that_resource,
                  url AS unique_url
                  FROM `%s`
                  GROUP BY unique_url
              )
              
              -- Add all other required columns to the already fetched unique resource crawls
              SELECT
              requestid,
              startedDateTime,
              firstReq,
              type,
              ext,
              url,
              status,
              LOWER(respOtherHeaders) resp_headers,
              
              FROM unique_desktop_1
              INNER JOIN `%s` ON unique_desktop_1.last_request_for_that_resource = requestid
            )
            
            
            UNION ALL ( -- Merge Desktop 1 & 2 data with Mobile data (UNION DISTINC works only for one column UNION, using UNION ALL here to avoid filtering out by requestId)
                WITH unique_mobile AS (
                  -- Fetch only the latest resource crawl for a unique url (resource)
                  SELECT
                  MAX(requestid) last_request_for_that_resource,
                  url AS unique_url
                  FROM `%s`
                  GROUP BY unique_url
              )
              
              -- Add all other required columns to the already fetched unique resource crawls
              SELECT
              requestid,
              startedDateTime,
              firstReq,
              type,
              ext,
              url,
              status,
              LOWER(respOtherHeaders) resp_headers,
              
              FROM unique_mobile
              INNER JOIN `%s` ON unique_mobile.last_request_for_that_resource = requestid
            )

            /* END base_table */
          )

          /* START merged_unique_url_table */
          -- DESC: UNION DISTINCT between desktop and mobile data does not work in this particular scenario
          --       Use another groupby to eliminate the duplicate url requests

          SELECT 
          requestid,
          startedDateTime,
          firstReq,
          type,
          ext,
          url,
          status,
          resp_headers,

          FROM base_table

          /* END merged_unique_url_table */
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

        (SELECT COUNT(*) FROM merged_unique_url_table) AS total_crawled_resources,
        (SELECT COUNT(*) FROM merged_unique_url_table WHERE firstReq = true) AS total_crawled_domains,

        REGEXP_CONTAINS(resp_headers, r"(?:^|.*[\s,]+)(nel\s*[=]\s*)") AS contains_nel,
        REGEXP_EXTRACT(resp_headers, r"(?:^|.*[\s,]+)nel\s*[=]\s*({.*?})") AS nel_value,

        FROM merged_unique_url_table
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
NET.PUBLIC_SUFFIX(url) as url_etld,
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

FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY url) unique_url_number
    FROM final_extracted_table
)

-- Filter out un-parseable data,
-- Data must have both report_to and NEL header
-- Filter out records containing either:
----     * non-json value,
----     * bad formatting (no value, missing brackets)
WHERE nel_report_to = rt_group 
      AND nel_report_to is not null 
      AND rt_group is not null
      AND unique_url_number = 1
/* END TOP LEVEL QUERY */
"""

QUERY_NEL_DATA_2_DESKTOP_2_MOBILE = r"""
WITH final_extracted_table AS (
  WITH rt_extracted_values_table AS (
    WITH nel_values_extracted_table AS (
      WITH nel_extracted_table AS (
        WITH merged_unique_url_table AS (
          WITH base_table AS (      
            /* START base_table */
            -- DESC: Fetch all the base columns necessary from both Desktop and Mobile variants of monthly data
            
            WITH unique_desktop_2 AS ( 
              -- Fetch only the latest resource crawl for a unique url (resource)
              SELECT
              MAX(requestid) last_request_for_that_resource,
              url AS unique_url
              FROM `%s`
              GROUP BY unique_url
              
            )
            
            -- Add all other required columns to the already fetched unique resource crawls
            SELECT
            requestid,
            firstReq,
            type,
            ext,
            url,
            status,
            LOWER(respOtherHeaders) resp_headers,

            FROM unique_desktop_2 
            INNER JOIN `%s` ON unique_desktop_2.last_request_for_that_resource = requestid


            UNION ALL ( -- Merge Desktop 1 data with Desktop 2 data (UNION DISTINC works only for one column UNION, using UNION ALL here to avoid filtering out by requestId)


              WITH unique_desktop_1 AS (
                  -- Fetch only the latest resource crawl for a unique url (resource)
                  SELECT
                  MAX(requestid) last_request_for_that_resource,
                  url AS unique_url
                  FROM `%s`
                  GROUP BY unique_url
              )
              
              -- Add all other required columns to the already fetched unique resource crawls
              SELECT
              requestid,
              firstReq,
              type,
              ext,
              url,
              status,
              LOWER(respOtherHeaders) resp_headers,
              
              FROM unique_desktop_1
              INNER JOIN `%s` ON unique_desktop_1.last_request_for_that_resource = requestid
            )
            
            
            UNION ALL ( -- Merge Desktop 1 & 2 data with Mobile 2 data (UNION DISTINC works only for one column UNION, using UNION ALL here to avoid filtering out by requestId)
                WITH unique_mobile_2 AS (
                  -- Fetch only the latest resource crawl for a unique url (resource)
                  SELECT
                  MAX(requestid) last_request_for_that_resource,
                  url AS unique_url
                  FROM `%s`
                  GROUP BY unique_url
              )
              
              -- Add all other required columns to the already fetched unique resource crawls
              SELECT
              requestid,
              firstReq,
              type,
              ext,
              url,
              status,
              LOWER(respOtherHeaders) resp_headers,
              
              FROM unique_mobile_2
              INNER JOIN `%s` ON unique_mobile_2.last_request_for_that_resource = requestid
            )
            
            
            UNION ALL ( -- Merge Desktop 1 & 2 and Mobile 2 data with Mobile 1 data (UNION DISTINC works only for one column UNION, using UNION ALL here to avoid filtering out by requestId)
                WITH unique_mobile_1 AS (
                  -- Fetch only the latest resource crawl for a unique url (resource)
                  SELECT
                  MAX(requestid) last_request_for_that_resource,
                  url AS unique_url
                  FROM `%s`
                  GROUP BY unique_url
              )
              
              -- Add all other required columns to the already fetched unique resource crawls
              SELECT
              requestid,
              firstReq,
              type,
              ext,
              url,
              status,
              LOWER(respOtherHeaders) resp_headers,
              
              FROM unique_mobile_1
              INNER JOIN `%s` ON unique_mobile_1.last_request_for_that_resource = requestid
            )

            /* END base_table */
          )

          /* START merged_unique_url_table */
          -- DESC: UNION DISTINCT between desktop and mobile data does not work in this particular scenario
          --       Use another groupby to eliminate the duplicate url requests

          SELECT 
          requestid,
          firstReq,
          type,
          ext,
          url,
          status,
          resp_headers,

          FROM base_table
          
          /* END merged_unique_url_table */
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

        (SELECT COUNT(*) FROM merged_unique_url_table) AS total_crawled_resources,
        (SELECT COUNT(*) FROM merged_unique_url_table WHERE firstReq = true) AS total_crawled_domains,

        REGEXP_CONTAINS(resp_headers, r"(?:^|.*[\s,]+)(nel\s*[=]\s*)") AS contains_nel,
        REGEXP_EXTRACT(resp_headers, r"(?:^|.*[\s,]+)nel\s*[=]\s*({.*?})") AS nel_value,

        FROM merged_unique_url_table
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
NET.PUBLIC_SUFFIX(url) as url_etld,
status,

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

FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY url) unique_url_number
    FROM final_extracted_table
)

-- Filter out un-parseable data,
-- Data must have both report_to and NEL header
-- Filter out records containing either:
----     * non-json value,
----     * bad formatting (no value, missing brackets)
WHERE nel_report_to = rt_group 
      AND nel_report_to is not null 
      AND rt_group is not null
      AND unique_url_number = 1

/* END TOP LEVEL QUERY */
"""
