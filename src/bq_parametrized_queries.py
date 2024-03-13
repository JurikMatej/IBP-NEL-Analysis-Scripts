QUERY_NEL_DATA_1_DESKTOP = r"""
WITH final_extracted_table AS (
  WITH rt_extracted_values_table AS (
     WITH nel_unique_resources_table AS (
        WITH nel_values_extracted_table AS (
          WITH nel_extracted_table AS (
            WITH base_table AS (     
              /* START base_table */
              -- DESC: Fetch all the base columns necessary from a single Desktop table
              
              WITH unique_desktop AS ( 
                -- Fetch only the latest resource request for a every unique url (resource)
                SELECT
                MAX(requestid) last_request_for_that_resource,
                url AS unique_url
                FROM `%s`
                GROUP BY unique_url
                
              )
              
              -- Add all other required columns from the same table 
              -- and filter by the already fetched unique resource request ids
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
            --  * Determine the number of 'total resource requests' and 'total unique domain resource requests'
            --  * Extract the NEL header value from the response
            SELECT
            requestid,
            firstReq,
            type,
            ext,
            url,
            status,
            resp_headers,
    
            -- Only one table in base_table (already filtered to unique resources) - no filtration to unique URLs needed
            -- Compute the numbers of unique resources
            (SELECT COUNT(*) FROM base_table) AS total_crawled_resources,

            -- Compute the number of unique domains among the unique resources crawled
            (SELECT COUNT(*) FROM (
              SELECT
                MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
                REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
              FROM base_table
              -- Grouping by the extracted url_domain leaves only the unique domains in this sub-select
              GROUP BY url_domain
            )) AS total_crawled_domains,
    

            REGEXP_CONTAINS(resp_headers, r"(?:^|.*[\s,]+)(nel\s*[=]\s*)") AS contains_nel,
            -- Non-json value
            -- OR bad formatting (no value, missing brackets)
            -- Will get picked up as NULL
            REGEXP_EXTRACT(resp_headers, r"(?:^|.*[\s,]+)nel\s*[=]\s*({.*?})") AS nel_value,
    
            FROM base_table
            /* END nel_extracted_table */
          )
    
          /* START nel_values_extracted_table */
          -- DESC:
          --    * Extract specific NEL fields from the nel_value
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
        
        /* START nel_unique_resources_table */
        -- DESC: Filter out duplicate NEL monitored resources by URL - keep only unique NEL resources (url rows)
    
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
    
        nel_value,
    
        nel_max_age,
        nel_failure_fraction,
        nel_success_fraction,
        nel_include_subdomains,
        nel_report_to,
         
        -- Add resource occurrence number to all resources 
        -- (1st time found in the data = 1; 2nd time found in the data = 2...)       
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (PARTITION BY url) unique_url_number
            FROM
            nel_values_extracted_table
        )
        
        -- Filter out duplicate resources (when all the NEL containing resources are already available)
        -- Take only the rows for which the resource occurrence is 1 (1st time occurring url in the data)
        WHERE unique_url_number = 1
        
        /* END nel_unique_resources_table */
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

    nel_max_age,
    nel_failure_fraction,
    nel_success_fraction,
    nel_include_subdomains,
    nel_report_to,

    -- Count unique resources with NEL
    -- (NEL that could also be incorrectly deployed, resources with correct NEL must be calculated from the result data)
    (SELECT COUNT(*) FROM nel_unique_resources_table) AS total_crawled_resources_with_nel,
    
    -- Count unique domains with NEL 
    -- (NEL that could also be incorrectly deployed, domains with correct NEL must be calculated from the result data)  
    (SELECT COUNT(*) FROM (
      SELECT
        MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
        REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
      FROM nel_unique_resources_table
      -- Grouping by the extracted url_domain leaves only the unique domains in this sub-select
      GROUP BY url_domain
    )) AS total_crawled_domains_with_nel,
    
    -- Non-json value
    -- OR bad formatting (no value, missing brackets)
    -- Will get picked up as NULL
    REGEXP_EXTRACT(
      resp_headers, 
      CONCAT(r"report[-]to\s*?[=].*([{](?:(?:[^\{]*?endpoints.*?[\[][^\[]*?[\]][^\}]*?)|(?:[^\{]*?endpoints.*?[\{][^\{]*?[\}]))?[^\]\}]*?group[\'\"][:]\s*?[\'\"]", nel_report_to, r"(?:(?:[^\}]*?endpoints[^\}]*?[\[][^\[]*?[\]][^\{]*?)|(?:[^\}]*?endpoints.*?[\{][^\{]*?[\}]))?.*?[}])")) 
    AS rt_value,

    FROM nel_unique_resources_table
    /* END rt_extracted_values_table */
    )

  /* START final_extracted_table */
  -- DESC: Extract Report-To specific fields from the rt_value
  
  SELECT
  requestid,
  firstReq,
  type,
  ext,
  url,
  status,

  total_crawled_resources,
  total_crawled_domains,

  nel_max_age,
  nel_failure_fraction,
  nel_success_fraction,
  nel_include_subdomains,
  nel_report_to,

  total_crawled_resources_with_nel,
  total_crawled_domains_with_nel,

  REGEXP_EXTRACT(rt_value, r".*group[\"\']\s*:\s*[\"\'](.+?)[\"\']") AS rt_group,

  REGEXP_EXTRACT_ALL(rt_value, r"url[\"\']\s*:\s*[\"\']http[s]?:[\\]*?[\/][\\]*?[\/]([^\/]+?)[\\]*?[\/\"]")
    AS rt_collectors,

  FROM rt_extracted_values_table
  /* END final_extracted_table */
)

/* START TOP LEVEL QUERY */
-- DESC: Perform the last updates to the data to be returned and filter out incorrect NEL usage

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
total_crawled_domains_with_nel,

rt_group,
rt_collectors,

FROM final_extracted_table

-- Finally, filter out incorrect NEL usage 
-- NEL.report-to must equal Report-To.group
WHERE nel_report_to = rt_group 
      AND nel_report_to is not null
      AND rt_group is not null
  
/* END TOP LEVEL QUERY */
"""


QUERY_NEL_DATA_1_DESKTOP_1_MOBILE = r"""
WITH final_extracted_table AS (
  WITH rt_extracted_values_table AS (
    WITH nel_unique_resources_table AS (
        WITH nel_values_extracted_table AS (
          WITH nel_extracted_table AS (
            WITH base_table AS (     
              /* START base_table */
              -- DESC: Fetch all the base columns necessary from both Desktop and Mobile variants of monthly data
                
              WITH unique_desktop AS ( 
                -- Fetch only the latest resource request for a every unique url (resource)
                SELECT
                MAX(requestid) last_request_for_that_resource,
                url AS unique_url
                FROM `%s`
                GROUP BY unique_url
                  
              )
                
              -- Add all other required columns from the same table 
              -- and filter by the already fetched unique resource request ids
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
    
    
              -- Merge Desktop data with Mobile data 
              -- (UNION DISTINC works only for one column unions, 
              --  using UNION ALL here to avoid filtering out by requestId)
              UNION ALL (
    
                WITH unique_mobile AS (
                    -- Fetch only the latest resource request for a every unique url (resource)
                    SELECT
                    MAX(requestid) last_request_for_that_resource,
                    url AS unique_url
                    FROM `%s`
                    GROUP BY unique_url
                )
                  
                -- Add all other required columns from the same table 
                -- and filter by the already fetched unique resource request ids
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
        
            /* START nel_extracted_table */
            -- DESC:
            --  * Determine the number of 'total resource requests' and 'total unique domain resource requests'
            --  * Extract the NEL header value from the response
            SELECT
            requestid,
            firstReq,
            type,
            ext,
            url,
            status,
            resp_headers,
    
            -- Calculate the total number of unique resources from base_table (base_table contains concatenated tables) 
            (SELECT COUNT(*) 
              FROM (
                  SELECT
                    MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
                    url as resource,
                  FROM base_table
                  -- Grouping by the resource leaves only unique resources in this sub-select
                  GROUP BY resource
              )                 
            ) 
            AS total_crawled_resources,
 
            -- Calculate the total number of unique domains from base_table (base_table contains concatenated tables) 
            (SELECT COUNT(*) 
              FROM (
                SELECT
                  MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
                  REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
                FROM base_table
                -- Grouping by the extracted url_domain leaves only the unique domains in this sub-select
                GROUP BY url_domain  
              )                 
            ) 
            AS total_crawled_domains,
    
            REGEXP_CONTAINS(resp_headers, r"(?:^|.*[\s,]+)(nel\s*[=]\s*)") AS contains_nel,
            -- Non-json value
            -- OR bad formatting (no value, missing brackets)
            -- Will get picked up as NULL
            REGEXP_EXTRACT(resp_headers, r"(?:^|.*[\s,]+)nel\s*[=]\s*({.*?})") AS nel_value,
    
            FROM base_table
            /* END nel_extracted_table */
          )
    
          /* START nel_values_extracted_table */
          -- DESC:
          --    * Extract specific NEL fields from the nel_value
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

        /* START nel_unique_resources_table */
        -- DESC: Filter out duplicate NEL monitored resources by URL - keep only unique NEL resources (url rows)
    
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
    
        nel_max_age,
        nel_failure_fraction,
        nel_success_fraction,
        nel_include_subdomains,
        nel_report_to,
         
        -- Add resource occurrence number to all resources 
        -- (1st time found in the data = 1; 2nd time found in the data = 2...)       
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (PARTITION BY url) unique_url_number
            FROM
            nel_values_extracted_table
        )
        
        -- Filter out duplicate resources (when all the NEL containing resources are already available)
        -- Take only the rows for which the resource occurrence is 1 (1st time occurring url in the data)
        WHERE unique_url_number = 1
        
        /* END nel_unique_resources_table */
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

    nel_max_age,
    nel_failure_fraction,
    nel_success_fraction,
    nel_include_subdomains,
    nel_report_to,

    -- Count unique resources with NEL
    -- (NEL that could also be incorrectly deployed, resources with correct NEL must be calculated from the result data)
    (SELECT COUNT(*) FROM nel_unique_resources_table) AS total_crawled_resources_with_nel,
    
    -- Count unique domains with NEL 
    -- (NEL that could also be incorrectly deployed, domains with correct NEL must be calculated from the result data)
    (SELECT COUNT(*) FROM (
      SELECT
        MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
        REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
      FROM nel_unique_resources_table
      -- Grouping by the extracted url_domain leaves only the unique domains in this sub-select
      GROUP BY url_domain
    )) AS total_crawled_domains_with_nel,
    
    -- Non-json value
    -- OR bad formatting (no value, missing brackets)
    -- Will get picked up as NULL
    REGEXP_EXTRACT(
        resp_headers, 
        CONCAT(r"report[-]to\s*?[=].*([{](?:(?:[^\{]*?endpoints.*?[\[][^\[]*?[\]][^\}]*?)|(?:[^\{]*?endpoints.*?[\{][^\{]*?[\}]))?[^\]\}]*?group[\'\"][:]\s*?[\'\"]", nel_report_to, r"(?:(?:[^\}]*?endpoints[^\}]*?[\[][^\[]*?[\]][^\{]*?)|(?:[^\}]*?endpoints.*?[\{][^\{]*?[\}]))?.*?[}])")) 
    AS rt_value,

    FROM nel_unique_resources_table
    /* END rt_extracted_values_table */
    )

  /* START final_extracted_table */
  -- DESC: Extract Report-To specific fields from the rt_value
  
  SELECT
  requestid,
  firstReq,
  type,
  ext,
  url,
  status,

  total_crawled_resources,
  total_crawled_domains,

  nel_max_age,
  nel_failure_fraction,
  nel_success_fraction,
  nel_include_subdomains,
  nel_report_to,

  total_crawled_resources_with_nel,
  total_crawled_domains_with_nel,

  REGEXP_EXTRACT(rt_value, r".*group[\"\']\s*:\s*[\"\'](.+?)[\"\']") AS rt_group,

  REGEXP_EXTRACT_ALL(rt_value, r"url[\"\']\s*:\s*[\"\']http[s]?:[\\]*?[\/][\\]*?[\/]([^\/]+?)[\\]*?[\/\"]")
    AS rt_collectors,

  FROM rt_extracted_values_table
  /* END final_extracted_table */
)

/* START TOP LEVEL QUERY */
-- DESC: Perform the last updates to the data to be returned and filter out incorrect NEL usage

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
total_crawled_domains_with_nel,

rt_group,
rt_collectors,

FROM final_extracted_table

-- Finally, filter out incorrect NEL usage 
-- NEL.report-to must equal Report-To.group
WHERE nel_report_to = rt_group 
      AND nel_report_to is not null 
      AND rt_group is not null 
/* END TOP LEVEL QUERY */
"""


QUERY_NEL_DATA_2_DESKTOP_1_MOBILE = r"""
WITH final_extracted_table AS (
  WITH rt_extracted_values_table AS (
    WITH nel_unique_resources_table AS (
      WITH nel_values_extracted_table AS (
        WITH nel_extracted_table AS (
          WITH base_table AS (      
            /* START base_table */
            -- DESC: Fetch all the base columns necessary from both Desktop 2 & 1 and Mobile 1 variants of monthly data
            
            WITH unique_desktop_2 AS ( 
              -- Fetch only the latest resource request for a every unique url (resource)
              SELECT
              MAX(requestid) last_request_for_that_resource,
              url AS unique_url
              FROM `%s`
              GROUP BY unique_url
              
            )
            
            -- Add all other required columns from the same table 
            -- and filter by the already fetched unique resource request ids
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

            -- Merge Desktop 2 data with Desktop 1 data 
            -- (UNION DISTINC works only for one column unions, 
            --  using UNION ALL here to avoid filtering out by requestId)
            UNION ALL (


              WITH unique_desktop_1 AS (
                  -- Fetch only the latest resource request for a every unique url (resource)
                  SELECT
                  MAX(requestid) last_request_for_that_resource,
                  url AS unique_url
                  FROM `%s`
                  GROUP BY unique_url
              )
              
              -- Add all other required columns from the same table 
              -- and filter by the already fetched unique resource request ids
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
            
            
            -- Merge Desktop 2 & 1 data with Mobile 1 data 
            -- (UNION DISTINC works only for one column unions, 
            --  using UNION ALL here to avoid filtering out by requestId)
            UNION ALL (
                WITH unique_mobile AS (
                  -- Fetch only the latest resource request for a every unique url (resource)
                  SELECT
                  MAX(requestid) last_request_for_that_resource,
                  url AS unique_url
                  FROM `%s`
                  GROUP BY unique_url
              )
              
              -- Add all other required columns from the same table 
              -- and filter by the already fetched unique resource request ids
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

          /* START nel_extracted_table */
          -- DESC:
          --  * Determine the number of 'total resource requests' and 'total unique domain resource requests'
          --  * Extract the NEL header value from the response
          SELECT
          requestid,
          firstReq,
          type,
          ext,
          url,
          status,
          resp_headers,

          -- Calculate the total number of unique resources from base_table (base_table contains concatenated tables) 
          (SELECT COUNT(*) 
            FROM (
                SELECT
                  MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
                  url as resource,
                FROM base_table
                -- Grouping by the resource leaves only unique resources in this sub-select
                GROUP BY resource
            )                 
          ) 
          AS total_crawled_resources,
 
          -- Calculate the total number of unique domains from base_table (base_table contains concatenated tables) 
          (SELECT COUNT(*) 
            FROM (
              SELECT
                MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
                REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
              FROM base_table
              -- Grouping by the extracted url_domain leaves only the unique domains in this sub-select
              GROUP BY url_domain  
            )                 
          ) 
          AS total_crawled_domains,

          REGEXP_CONTAINS(resp_headers, r"(?:^|.*[\s,]+)(nel\s*[=]\s*)") AS contains_nel,
          -- Non-json value
          -- OR bad formatting (no value, missing brackets)
          -- Will get picked up as NULL
          REGEXP_EXTRACT(resp_headers, r"(?:^|.*[\s,]+)nel\s*[=]\s*({.*?})") AS nel_value,

          FROM base_table
          /* END nel_extracted_table */
        )

        /* START nel_values_extracted_table */
        -- DESC:
        --    * Extract specific NEL fields from the nel_value
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
        
      /* START nel_unique_resources_table */
      -- DESC: Filter out duplicate NEL monitored resources by URL - keep only unique NEL resources (url rows)
    
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
    
      nel_max_age,
      nel_failure_fraction,
      nel_success_fraction,
      nel_include_subdomains,
      nel_report_to,
         
      -- Add resource occurrence number to all resources 
      -- (1st time found in the data = 1; 2nd time found in the data = 2...)       
      FROM (
          SELECT 
              *,
              ROW_NUMBER() OVER (PARTITION BY url) unique_url_number
          FROM
          nel_values_extracted_table
      )
        
      -- Filter out duplicate resources (when all the NEL containing resources are already available)
      -- Take only the rows for which the resource occurrence is 1 (1st time occurring url in the data)
      WHERE unique_url_number = 1
        
      /* END nel_unique_resources_table */
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

    nel_max_age,
    nel_failure_fraction,
    nel_success_fraction,
    nel_include_subdomains,
    nel_report_to,

    -- Count unique resources with NEL
    -- (NEL that could also be incorrectly deployed, resources with correct NEL must be calculated from the result data)
    (SELECT COUNT(*) FROM nel_unique_resources_table) AS total_crawled_resources_with_nel,
    
    -- Count unique domains with NEL 
    -- (NEL that could also be incorrectly deployed, domains with correct NEL must be calculated from the result data) 
    (SELECT COUNT(*) FROM (
      SELECT
        MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
        REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
      FROM nel_unique_resources_table
      -- Grouping by the extracted url_domain leaves only the unique domains in this sub-select
      GROUP BY url_domain
    )) AS total_crawled_domains_with_nel,
    
    -- Non-json value
    -- OR bad formatting (no value, missing brackets)
    -- Will get picked up as NULL
    REGEXP_EXTRACT(
        resp_headers, 
        CONCAT(r"report[-]to\s*?[=].*([{](?:(?:[^\{]*?endpoints.*?[\[][^\[]*?[\]][^\}]*?)|(?:[^\{]*?endpoints.*?[\{][^\{]*?[\}]))?[^\]\}]*?group[\'\"][:]\s*?[\'\"]", nel_report_to, r"(?:(?:[^\}]*?endpoints[^\}]*?[\[][^\[]*?[\]][^\{]*?)|(?:[^\}]*?endpoints.*?[\{][^\{]*?[\}]))?.*?[}])")) 
    AS rt_value,

    FROM nel_unique_resources_table
    /* END rt_extracted_values_table */
    )

  /* START final_extracted_table */
  -- DESC: Extract Report-To specific fields from the rt_value
  
  SELECT
  requestid,
  firstReq,
  type,
  ext,
  url,
  status,

  total_crawled_resources,
  total_crawled_domains,

  nel_max_age,
  nel_failure_fraction,
  nel_success_fraction,
  nel_include_subdomains,
  nel_report_to,

  total_crawled_resources_with_nel,
  total_crawled_domains_with_nel,

  REGEXP_EXTRACT(rt_value, r".*group[\"\']\s*:\s*[\"\'](.+?)[\"\']") AS rt_group,

  REGEXP_EXTRACT_ALL(rt_value, r"url[\"\']\s*:\s*[\"\']http[s]?:[\\]*?[\/][\\]*?[\/]([^\/]+?)[\\]*?[\/\"]")
    AS rt_collectors,

  FROM rt_extracted_values_table
  /* END final_extracted_table */
)

/* START TOP LEVEL QUERY */
-- DESC: Perform the last updates to the data to be returned and filter out incorrect NEL usage
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
total_crawled_domains_with_nel,

rt_group,
rt_collectors,

FROM final_extracted_table

-- Finally, filter out incorrect NEL usage 
-- NEL.report-to must equal Report-To.group
WHERE nel_report_to = rt_group 
      AND nel_report_to is not null 
      AND rt_group is not null 
      
/* END TOP LEVEL QUERY */
"""


QUERY_NEL_DATA_2_DESKTOP_2_MOBILE = r"""
WITH final_extracted_table AS (
  WITH rt_extracted_values_table AS (
    WITH nel_unique_resources_table AS (
      WITH nel_values_extracted_table AS (
        WITH nel_extracted_table AS (
          WITH base_table AS (      
            /* START base_table */
            -- DESC: Fetch all the base columns necessary from both 
            --       Desktop 2 & 1 and Mobile 2 & 1 variants of monthly data
            
            WITH unique_desktop_2 AS ( 
              -- Fetch only the latest resource request for a every unique url (resource)
              SELECT
              MAX(requestid) last_request_for_that_resource,
              url AS unique_url
              FROM `%s`
              GROUP BY unique_url
              
            )
            
            -- Add all other required columns from the same table 
            -- and filter by the already fetched unique resource request ids
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


            -- Merge Desktop 2 data with Desktop 1 data 
            -- (UNION DISTINC works only for one column unions, 
            --  using UNION ALL here to avoid filtering out by requestId)
            UNION ALL (

              WITH unique_desktop_1 AS (
                  -- Fetch only the latest resource request for a every unique url (resource)
                  SELECT
                  MAX(requestid) last_request_for_that_resource,
                  url AS unique_url
                  FROM `%s`
                  GROUP BY unique_url
              )
              
              -- Add all other required columns from the same table 
              -- and filter by the already fetched unique resource request ids
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
            
            -- Merge Desktop 2 & 1 data with Mobile 2 data 
            -- (UNION DISTINC works only for one column unions, 
            --  using UNION ALL here to avoid filtering out by requestId)
            UNION ALL (
            
                WITH unique_mobile_2 AS (
                  -- Fetch only the latest resource request for a every unique url (resource)
                  SELECT
                  MAX(requestid) last_request_for_that_resource,
                  url AS unique_url
                  FROM `%s`
                  GROUP BY unique_url
                )
              
                -- Add all other required columns from the same table 
                -- and filter by the already fetched unique resource request ids
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
            
            
            -- Merge Desktop 2 & 1 data with Mobile 2 & 1 data 
            -- (UNION DISTINC works only for one column unions, 
            --  using UNION ALL here to avoid filtering out by requestId)
            UNION ALL (
            
                WITH unique_mobile_1 AS (
                  -- Fetch only the latest resource request for a every unique url (resource)
                  SELECT
                  MAX(requestid) last_request_for_that_resource,
                  url AS unique_url
                  FROM `%s`
                  GROUP BY unique_url
                )
              
                -- Add all other required columns from the same table 
                -- and filter by the already fetched unique resource request ids
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

          /* START nel_extracted_table */
          -- DESC:
          --  * Determine the number of 'total resource requests' and 'total unique domain resource requests'
          --  * Extract the NEL header value from the response
          SELECT
          requestid,
          firstReq,
          type,
          ext,
          url,
          status,
          resp_headers,

          -- Calculate the total number of unique resources from base_table (base_table contains concatenated tables) 
          (SELECT COUNT(*) 
            FROM (
                SELECT
                  MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
                  url as resource,
                FROM base_table
                -- Grouping by the resource leaves only unique resources in this sub-select
                GROUP BY resource
            )                 
          ) 
          AS total_crawled_resources,
 
          -- Calculate the total number of unique domains from base_table (base_table contains concatenated tables) 
          (SELECT COUNT(*) 
            FROM (
              SELECT
                MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
                REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
              FROM base_table
              -- Grouping by the extracted url_domain leaves only the unique domains in this sub-select
              GROUP BY url_domain  
            )                 
          ) 
          AS total_crawled_domains,
          
          REGEXP_CONTAINS(resp_headers, r"(?:^|.*[\s,]+)(nel\s*[=]\s*)") AS contains_nel,
          -- Non-json value
          -- OR bad formatting (no value, missing brackets)
          -- Will get picked up as NULL
          REGEXP_EXTRACT(resp_headers, r"(?:^|.*[\s,]+)nel\s*[=]\s*({.*?})") AS nel_value,

          FROM base_table
          /* END nel_extracted_table */
        )

        /* START nel_values_extracted_table */
        -- DESC:
        --    * Extract specific NEL fields from the nel_value
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
      
      /* START nel_unique_resources_table */
      -- DESC: Filter out duplicate NEL monitored resources by URL - keep only unique NEL resources (url rows)
    
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
    
      nel_max_age,
      nel_failure_fraction,
      nel_success_fraction,
      nel_include_subdomains,
      nel_report_to,
         
      -- Add resource occurrence number to all resources 
      -- (1st time found in the data = 1; 2nd time found in the data = 2...)       
      FROM (
          SELECT 
              *,
              ROW_NUMBER() OVER (PARTITION BY url) unique_url_number
          FROM
          nel_values_extracted_table
      )
        
      -- Filter out duplicate resources (when all the NEL containing resources are already available)
      -- Take only the rows for which the resource occurrence is 1 (1st time occurring url in the data)
      WHERE unique_url_number = 1
        
      /* END nel_unique_resources_table */
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

    nel_max_age,
    nel_failure_fraction,
    nel_success_fraction,
    nel_include_subdomains,
    nel_report_to,

    -- Count unique resources with NEL
    -- (NEL that could also be incorrectly deployed, resources with correct NEL must be calculated from the result data)
    (SELECT COUNT(*) FROM nel_unique_resources_table) AS total_crawled_resources_with_nel,
    
    -- Count unique domains with NEL 
    -- (NEL that could also be incorrectly deployed, domains with correct NEL must be calculated from the result data)  
    (SELECT COUNT(*) FROM (
      SELECT
        MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
        REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
      FROM nel_unique_resources_table
      -- Grouping by the extracted url_domain leaves only the unique domains in this sub-select
      GROUP BY url_domain
    )) AS total_crawled_domains_with_nel,

    -- Non-json value
    -- OR bad formatting (no value, missing brackets)
    -- Will get picked up as NULL
    REGEXP_EXTRACT(
        resp_headers, 
        CONCAT(r"report[-]to\s*?[=].*([{](?:(?:[^\{]*?endpoints.*?[\[][^\[]*?[\]][^\}]*?)|(?:[^\{]*?endpoints.*?[\{][^\{]*?[\}]))?[^\]\}]*?group[\'\"][:]\s*?[\'\"]", nel_report_to, r"(?:(?:[^\}]*?endpoints[^\}]*?[\[][^\[]*?[\]][^\{]*?)|(?:[^\}]*?endpoints.*?[\{][^\{]*?[\}]))?.*?[}])")) 
    AS rt_value,

    FROM nel_unique_resources_table
    /* END rt_extracted_values_table */
  )

  /* START final_extracted_table */
  -- DESC: Extract Report-To specific fields from the rt_value
  
  SELECT
  requestid,
  firstReq,
  type,
  ext,
  url,
  status,

  total_crawled_resources,
  total_crawled_domains,

  nel_max_age,
  nel_failure_fraction,
  nel_success_fraction,
  nel_include_subdomains,
  nel_report_to,

  total_crawled_resources_with_nel,
  total_crawled_domains_with_nel,

  REGEXP_EXTRACT(rt_value, r".*group[\"\']\s*:\s*[\"\'](.+?)[\"\']") AS rt_group,

  REGEXP_EXTRACT_ALL(rt_value, r"url[\"\']\s*:\s*[\"\']http[s]?:[\\]*?[\/][\\]*?[\/]([^\/]+?)[\\]*?[\/\"]")
    AS rt_collectors,

  FROM rt_extracted_values_table
  /* END final_extracted_table */
)

/* START TOP LEVEL QUERY */
-- DESC: Perform the last updates to the data to be returned and filter out incorrect NEL usage
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
total_crawled_domains_with_nel,

rt_group,
rt_collectors,

FROM final_extracted_table

-- Finally, filter out incorrect NEL usage 
-- NEL.report-to must equal Report-To.group
WHERE nel_report_to = rt_group 
      AND nel_report_to is not null 
      AND rt_group is not null

/* END TOP LEVEL QUERY */
"""
