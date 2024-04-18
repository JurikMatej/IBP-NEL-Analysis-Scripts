QUERY_NEL_DATA_HEADER_1_DESKTOP = r"""
-- SELECT ALL DATA TO BE PROCESSED
WITH httparchive_full_month AS (
  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
    status,
    LOWER(respOtherHeaders) resp_headers,

  FROM `%s`
)
"""

QUERY_NEL_DATA_HEADER_1_DESKTOP_1_MOBILE = r"""
-- SELECT ALL DATA TO BE PROCESSED
WITH httparchive_full_month AS (
  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
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
    REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
    status,
    LOWER(respOtherHeaders) resp_headers,
    
  FROM `%s`
)
"""

QUERY_NEL_DATA_HEADER_2_DESKTOP_1_MOBILE = r"""
-- SELECT ALL DATA TO BE PROCESSED
WITH httparchive_full_month AS (
  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
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
    REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
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
    REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
    status,
    LOWER(respOtherHeaders) resp_headers,
    
  FROM `%s`
)
"""

QUERY_NEL_DATA_HEADER_2_DESKTOP_2_MOBILE = r"""
-- SELECT ALL DATA TO BE PROCESSED
WITH httparchive_full_month AS (
  SELECT 
    requestid,
    firstReq,
    type,
    ext,
    url,
    REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
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
    REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
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
    REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
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
    REGEXP_EXTRACT(url, r"http[s]?:[\/][\/]([^\/:]+)") AS url_domain,
    status,
    LOWER(respOtherHeaders) resp_headers,
    
  FROM `%s`
)
"""




QUERY_NEL_DATA_BODY = r"""
-- THIS IS THE TOP LEVEL SELECT -- THIS MUST COMPLY TO THE ANALYSIS DATA CONTRACT
SELECT
  requestId,
  firstReq,
  type,
  ext,
  status,

  url,
  url_domain,
  url_domain_registrable,
  
  url_domain_hosted_resources,
  url_domain_hosted_resources_with_nel,
  
  url_domain_monitored_resources_ratio,

  total_crawled_resources,
  total_crawled_domains,

  total_crawled_resources_with_nel,
  total_crawled_domains_with_nel,

  total_crawled_resources_with_correct_nel,
  total_crawled_domains_with_correct_nel,

  nel_max_age,
  nel_failure_fraction,
  nel_success_fraction,
  nel_include_subdomains,
  nel_report_to,

  rt_collectors,
  rt_collectors_registrable

FROM (
    
  WITH -- ...DEFINE ALL DATA PROCESSING STEPS AS INTERMEDIATE TABLES 
  
  
  unique_total_counting_table AS (
    /* START unique_total_counting_table */ 
    SELECT
      requestId,
      firstReq,
      type,
      ext,
      status,
      resp_headers,
      url,
      url_domain,


      -- Calculate the total number of unique resources hosted by each domain
      COUNT(DISTINCT url) OVER (PARTITION BY url_domain) url_domain_hosted_resources,


      -- Calculate the total number of unique resources from base_table (base_table contains concatenated tables) 
      (SELECT COUNT(*)
        FROM (
            SELECT DISTINCT url FROM httparchive_full_month
        )
      )
      AS total_crawled_resources,


      -- Calculate the total number of unique domains from base_table (base_table contains concatenated tables) 
      (SELECT COUNT(*)
        FROM (
          SELECT
            MIN(requestid) _,  -- irrelevant - used only for aggregation of group by
            url_domain,
          FROM httparchive_full_month
        
          -- Grouping by the extracted url_domain leaves only the unique domains in this sub-select
          GROUP BY url_domain  
        )                 
      ) 
      AS total_crawled_domains,


    FROM httparchive_full_month
    ORDER BY url_domain

    /* END unique_total_counting_table */
  )


  , nel_header_extracting_table AS (
    /* START nel_header_extracting_table */
    SELECT
      requestId,
      firstReq,
      type,
      ext,
      status,
      resp_headers,
      url,
      url_domain,

      url_domain_hosted_resources,

      total_crawled_resources,
      total_crawled_domains,

      REGEXP_CONTAINS(resp_headers, r"(?:^|.*[\s,]+)(nel\s*[=]\s*)") AS contains_nel,
      -- Non-json value
      -- OR bad formatting (no value, missing brackets)
      -- Will get picked up as NULL
      REGEXP_EXTRACT(resp_headers, r"(?:^|.*[\s,]+)nel\s*[=]\s*({.*?})") AS nel_value,

    FROM unique_total_counting_table
    /* END nel_header_extracting_table */
  )

  
  , nel_field_extracting_table AS (
    /* START nel_field_extracting_table */ 
    SELECT
      requestId,
      firstReq,
      type,
      ext,
      status,
      resp_headers,
      url,
      url_domain,

      url_domain_hosted_resources,

      total_crawled_resources,
      total_crawled_domains,

      -- Extract NEL values
      REGEXP_EXTRACT(nel_value, r".*max_age[\"\']\s*:\s*([0-9]+)")              AS nel_max_age,
      REGEXP_EXTRACT(nel_value, r".*failure[_]fraction[\"\']\s*:\s*([0-9\.]+)") AS nel_failure_fraction,
      REGEXP_EXTRACT(nel_value, r".*success[_]fraction[\"\']\s*:\s*([0-9\.]+)") AS nel_success_fraction,
      REGEXP_EXTRACT(nel_value, r".*include[_]subdomains[\"\']\s*:\s*(\w+)")    AS nel_include_subdomains,

      REGEXP_EXTRACT(nel_value, r".*report_to[\"\']\s*:\s*[\"\'](.+?)[\"\']")   AS nel_report_to,

    FROM nel_header_extracting_table

    -- Filter out the non-NEL responses
    WHERE contains_nel = true
    /* END nel_field_extracting_table */ 
  )
  
  , unique_nel_resources_table AS (
    /* START unique_nel_resources_table */ 
    SELECT
      requestId,
      firstReq,
      type,
      ext,
      status,
      resp_headers,
      url,
      url_domain,

      url_domain_hosted_resources,
    
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
        requestId,
        firstReq,
        type,
        ext,
        status,
        resp_headers,
        url,
        url_domain,

        url_domain_hosted_resources,
    
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
        FROM nel_field_extracting_table
        GROUP BY unique_resource
      ) AS unique_resource_table
      
      INNER JOIN nel_field_extracting_table ON requestid = unique_resource_table.latest_request_id
      ORDER BY url_domain
    )
    /* END unique_nel_resources_table */    
  )
  
  , unique_nel_total_counting_table AS (
    /* START unique_nel_total_counting_table */ 
    SELECT
      requestId,
      firstReq,
      type,
      ext,
      status,
      resp_headers,
      url,
      url_domain,
      
      url_domain_hosted_resources,

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
          url_domain,
        FROM unique_nel_resources_table
        -- Grouping by the extracted url_domain leaves only the unique domains in this sub-select
        GROUP BY url_domain
      )) AS total_crawled_domains_with_nel,
      
    
    FROM unique_nel_resources_table
    /* END unique_nel_total_counting_table */ 
  )


  , nel_url_domain_hosted_nel_resources_counting_table AS (
    /* START nel_url_domain_hosted_nel_resources_counting_table */
    SELECT
      requestId,
      firstReq,
      type,
      ext,
      status,
      resp_headers,
      url,
      url_domain,
      
      url_domain_hosted_resources,
      COUNT(DISTINCT url) OVER (PARTITION BY url_domain) url_domain_hosted_resources_with_nel,

      total_crawled_resources,
      total_crawled_domains,

      total_crawled_resources_with_nel,
      total_crawled_domains_with_nel,
      
      nel_max_age,
      nel_failure_fraction,
      nel_success_fraction,
      nel_include_subdomains,
      nel_report_to,
    
    FROM unique_nel_total_counting_table
    /* END nel_url_domain_hosted_nel_resources_counting_table */
  )


  , rt_header_extracting_table AS (
    SELECT 
      /* START rt_header_extracting_table */
      requestId,
      firstReq,
      type,
      ext,
      status,
      url,
      url_domain,
      
      url_domain_hosted_resources,
      url_domain_hosted_resources_with_nel,

      total_crawled_resources,
      total_crawled_domains,
      total_crawled_resources_with_nel,
      total_crawled_domains_with_nel,

      nel_max_age,
      nel_failure_fraction,
      nel_success_fraction,
      nel_include_subdomains,
      nel_report_to,

      -- Non-json value
      -- OR bad formatting (no value, missing brackets)
      -- Will get picked up as NULL
      REGEXP_EXTRACT(
          resp_headers, 
          CONCAT(r"report[-]to\s*?[=].*([{](?:(?:[^\{]*?endpoints.*?[\[][^\[]*?[\]][^\}]*?)|(?:[^\{]*?endpoints.*?[\{][^\{]*?[\}]))?[^\]\}]*?group[\'\"][:]\s*?[\'\"]", nel_report_to, r"(?:(?:[^\}]*?endpoints[^\}]*?[\[][^\[]*?[\]][^\{]*?)|(?:[^\}]*?endpoints.*?[\{][^\{]*?[\}]))?.*?[}])")) 
      AS rt_value,

    FROM nel_url_domain_hosted_nel_resources_counting_table
    /* END rt_header_extracting_table */
  )
  

  , rt_field_extracting_table AS (
    /* START rt_field_extracting_table */
    SELECT
      requestId,
      firstReq,
      type,
      ext,
      status,
      url,
      url_domain,

      url_domain_hosted_resources,
      url_domain_hosted_resources_with_nel,

      total_crawled_resources,
      total_crawled_domains,
      total_crawled_resources_with_nel,
      total_crawled_domains_with_nel,

      nel_max_age,
      nel_failure_fraction,
      nel_success_fraction,
      nel_include_subdomains,
      nel_report_to,

      REGEXP_EXTRACT(rt_value, r".*group[\"\']\s*:\s*[\"\'](.+?)[\"\']") AS rt_group,

      REGEXP_EXTRACT_ALL(rt_value, r"url[\"\']\s*:\s*[\"\']http[s]?:[\\]*?[\/][\\]*?[\/]([^\/]+?)[\\]*?[\/\"]")
        AS rt_collectors,

    FROM rt_header_extracting_table
    /* END rt_field_extracting_table */
  )
  

  , rt_collectors_registrable_domain_table AS (
    /* START rt_collectors_registrable_domain_table */
    SELECT
      requestId,
      firstReq,
      type,
      ext,
      status,
      url,
      url_domain,

      url_domain_hosted_resources,
      url_domain_hosted_resources_with_nel,

      total_crawled_resources,
      total_crawled_domains,
      total_crawled_resources_with_nel,
      total_crawled_domains_with_nel,

      nel_max_age,
      nel_failure_fraction,
      nel_success_fraction,
      nel_include_subdomains,
      nel_report_to,

      rt_group,
      rt_collectors,

      -- Parse either:
      --    1. The registrable collector domains using an UP-TO-DATE Public Suffix List, 
      --    2. Or, upon PSL parse failure - fallback to TLD + second to last domain label
      ARRAY(
        (SELECT
          IFNULL( 
            IFNULL(
              IFNULL(
                NET.REG_DOMAIN(rt_collector), 
                REGEXP_EXTRACT(rt_collector, r"\.(\w+\.\w+$)") -- Extract TLD + second to last domain label
              ), 
              rt_collector  -- Fallback to the original collector name if neither of the above works
            ),
            '' -- Fallback to empty string if everything went wrong
          )
          AS rt_collector_registrable 
        FROM 
          UNNEST(rt_collectors) AS rt_collector)
      ) 
      AS rt_collectors_registrable

    FROM rt_field_extracting_table
    /* END rt_collectors_registrable_domain_table */
  )
  

  , final_modifications_table AS (
    /* START final_modifications_table */ 
    SELECT
      requestId,
      firstReq,
      type,
      ext,
      status,
      url,
      url_domain,
      IFNULL(
        NET.REG_DOMAIN(url_domain), 
        REGEXP_EXTRACT(url_domain, r"\.(\w+\.\w+$)") -- Extract TLD + second to last domain label
      ) 
      AS url_domain_registrable,

      url_domain_hosted_resources,
      url_domain_hosted_resources_with_nel,
      ROUND(
        SAFE_MULTIPLY(
          SAFE_DIVIDE(url_domain_hosted_resources_with_nel, url_domain_hosted_resources), 
        100), 
      2) 
      AS url_domain_monitored_resources_ratio,

      total_crawled_resources,
      total_crawled_domains,
      
      total_crawled_resources_with_nel,
      total_crawled_domains_with_nel,
    
      nel_max_age,
      
      CASE 
        -- Make 0 and 1 (both end-values) always of length 3
        WHEN nel_failure_fraction = '0' THEN '0.0' 
        WHEN nel_failure_fraction = '1' THEN '1.0'
        
        -- Coallesce NULL values to default value
        WHEN nel_failure_fraction IS NULL THEN '1.0'

        -- Otherwise keep as is
        ELSE nel_failure_fraction
      END
      AS nel_failure_fraction,
      
      CASE 
        -- Make 0 and 1 (both end-values) always of length 3
        WHEN nel_success_fraction = '0' THEN '0.0' 
        WHEN nel_success_fraction = '1' THEN '1.0'
        
        -- Coallesce NULL values to default value
        WHEN nel_success_fraction IS NULL THEN '0.0'

        -- Otherwise keep as is
        ELSE nel_success_fraction
      END
      AS nel_success_fraction,

      IFNULL(nel_include_subdomains, 'false') AS nel_include_subdomains,
      nel_report_to,
    
      rt_collectors,
      rt_collectors_registrable

    
    FROM rt_collectors_registrable_domain_table
    
    -- Finally, filter out incorrect NEL usage 
    -- NEL.report-to must equal Report-To.group
    WHERE nel_report_to = rt_group 
          AND nel_report_to IS NOT NULL
          AND rt_group IS NOT NULL 

    /* END final_modifications_table */
  )

  /* START --TOP LEVEL QUERY-- */
  SELECT 
    requestId,
    firstReq,
    type,
    ext,
    status,
    url,
    url_domain,
    url_domain_registrable,

    url_domain_hosted_resources,
    url_domain_hosted_resources_with_nel,
    url_domain_monitored_resources_ratio,

    total_crawled_resources,
    total_crawled_domains,
    
    total_crawled_resources_with_nel,
    total_crawled_domains_with_nel,

    (SELECT COUNT(url) FROM final_modifications_table) AS total_crawled_resources_with_correct_nel,
    
    (SELECT 
      COUNT(url_domain) 
     FROM (
        SELECT 
          MAX(requestid),
          url_domain,
        FROM final_modifications_table
        GROUP BY url_domain
     )
    )
    AS total_crawled_domains_with_correct_nel,
  
    nel_max_age,
    nel_failure_fraction,
    nel_success_fraction,
    nel_include_subdomains,
    nel_report_to,
  
    rt_collectors,
    rt_collectors_registrable

  FROM final_modifications_table
  ORDER BY url_domain ASC
  /* END --TOP LEVEL QUERY-- */
)
"""
