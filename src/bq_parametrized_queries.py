QUERY_NEL_DATA_HEADER_1_DESKTOP = r"""
-- SELECT ALL DATA TO BE PROCESSED
WITH all_data_table AS (
  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    status,
    LOWER(respOtherHeaders) resp_headers,

  FROM `%s`
)
"""

QUERY_NEL_DATA_HEADER_1_DESKTOP_1_MOBILE = r"""
-- SELECT ALL DATA TO BE PROCESSED
WITH all_data_table AS (
  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    status,
    LOWER(respOtherHeaders) resp_headers,

  FROM `%s`
  
  UNION ALL 

  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    status,
    LOWER(respOtherHeaders) resp_headers,
    
  FROM `%s`
)
"""

QUERY_NEL_DATA_HEADER_2_DESKTOP_1_MOBILE = r"""
-- SELECT ALL DATA TO BE PROCESSED
WITH all_data_table AS (
  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    status,
    LOWER(respOtherHeaders) resp_headers,

  FROM `%s`
  
  UNION ALL 

  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    status,
    LOWER(respOtherHeaders) resp_headers,
    
  FROM `%s`

  UNION ALL 

  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    status,
    LOWER(respOtherHeaders) resp_headers,
    
  FROM `%s`
)
"""


QUERY_NEL_DATA_HEADER_2_DESKTOP_2_MOBILE = r"""
-- SELECT ALL DATA TO BE PROCESSED
WITH all_data_table AS (
  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    status,
    LOWER(respOtherHeaders) resp_headers,

  FROM `%s`
  
  UNION ALL 

  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    status,
    LOWER(respOtherHeaders) resp_headers,
    
  FROM `%s`

  UNION ALL 

  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    status,
    LOWER(respOtherHeaders) resp_headers,
    
  FROM `%s`
  
  UNION ALL 

  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    status,
    LOWER(respOtherHeaders) resp_headers,
    
  FROM `%s`
)
"""


QUERY_NEL_DATA_BODY = r"""
-- THIS IS THE TOP LEVEL SELECT -- THIS MUST COMPLY TO THE ANALYSIS DATA CONTRACT
SELECT
  requestid,
  firstReq,
  type,
  ext,
  url,
  url_etld,
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

FROM (
    
  WITH -- ...DEFINE ALL DATA PROCESSING STEPS AS INTERMEDIATE TABLES 
  
  all_totals_table AS (
    /* START all_totals_table */ 
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
            SELECT DISTINCT url FROM all_data_table
        )
      )
      AS total_crawled_resources,

      -- Calculate the total number of unique domains from base_table (base_table contains concatenated tables) 
      (SELECT COUNT(*)
        FROM (
          SELECT
            MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
            REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
          FROM all_data_table
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

    FROM all_data_table
    /* END all_totals_table */
  )
  
  , nel_extracted_table AS (
    /* START nel_extracted_table */ 
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

    FROM all_totals_table

    -- Filter out the non-NEL responses
    WHERE contains_nel = true
    /* END nel_extracted_table */ 
  )
  
  , unique_nel_resources_table AS (
    /* START unique_nel_resources_table */ 
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
        
    -- Create a table with only unique NEL containing resources to select from   
    FROM (
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
          
      FROM (
        SELECT 
          MAX(requestid) latest_request_id,
          url as unique_resource
        FROM nel_extracted_table
        GROUP BY unique_resource
      ) AS unique_resource_table
      
      INNER JOIN nel_extracted_table ON requestid = unique_resource_table.latest_request_id
    )
    /* END unique_nel_resources_table */    
  )
  
  , unique_nel_totals_table AS (
    /* START unique_nel_totals_table */ 
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
      (SELECT COUNT(*) FROM unique_nel_resources_table) AS total_crawled_resources_with_nel,
      
      -- Count unique domains with NEL 
      -- (NEL that could also be incorrectly deployed, domains with correct NEL must be calculated from the result data)
      (SELECT COUNT(*) FROM (
        SELECT
          MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
          REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
        FROM unique_nel_resources_table
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
    
    FROM unique_nel_resources_table
    /* END unique_nel_totals_table */ 
  )
  
  , rt_extracted_unique_nel_table AS (
    /* START rt_extracted_unique_nel_table */ 
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

    FROM unique_nel_totals_table
    /* END rt_extracted_unique_nel_table */ 
  )
  
  /* START --TOP LEVEL QUERY-- */ 
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
  
  FROM rt_extracted_unique_nel_table
  
  -- Finally, filter out incorrect NEL usage 
  -- NEL.report-to must equal Report-To.group
  WHERE nel_report_to = rt_group 
        AND nel_report_to is not null 
        AND rt_group is not null 
  
  /* END --TOP LEVEL QUERY-- */
)
"""
