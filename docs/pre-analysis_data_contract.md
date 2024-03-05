# Pre-Analysis Data Contract

This document describes what data is expected by the `Analyze` part of the analysis lifecycle.
Its purpose is to make it easier to write implementation for all metric-computing functions with sample data _early_.

Some issues occurred while working on the `Download` and `Postprocess` lifecycle parts
that cannot be resolved until the next iteration of work (meeting with thesis supervisor).
This document was created to abstract the required output of those two parts (data structure and its content)
from the downstream processes following the `Postprocess` stage.
With this contract in place, the output of the first 2 stages SHOULD be mock-able so that
`Analyze` stage is implemented first.
And with `Analyze` implemented, only the mentioned problems are left to be resolved and the mocked
data replaced with the real implementation able to produce required data.

## Schema Definitions

### Description

This section describes structure of the data required for the `Analyze` stage - analysis data.
Analysis data is obtained from the HTTP Archive project and modified by upstream stages (`Download` and `Postprocess`
stages for this project).

Representing the minimal sample for NEL deployment state analysis, analysis data contains only the crawled request &
response pairs (with additional metadata) in which the response **contained both `NEL` and `Report-To`** HTTP response
headers.
That is, analysis data **only contains correctly functional NEL** deployment instances.

NEL deployment is correctly functional only when HTTP response from the target web server contains a valid `NEL` and
a valid `Report-To` headers with matching `NEL.report-to` and `Report-To.group` HTTP header values.

Abbreviation Legend:

| Abbrev. | Full name             |
|:--------|:----------------------|
| nel     | Network Error Logging |
| rt      | Report To             |

### Schema

| Key                              |  Type   | Default To | Description                                                                                                                                                                                                                  |
|:---------------------------------|:-------:|:----------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| requestId                        | INTEGER |     -      | Primary key - The particular HTTP Archive crawl request's ID                                                                                                                                                                 |
| firstReq                         |  BOOL   |     -      | Flag indicating whether that request was the first request made to the domain - {url} field                                                                                                                                  |
| type                             | STRING  |     -      | Requested resource type (html, script, video, font)                                                                                                                                                                          |
| ext                              | STRING  |     -      | Requested resource file extension without the leading dot (html, json, mp4, woff2)                                                                                                                                           |
| url                              | STRING  |     -      | HTTP Archive crawl's requested URL (https://www.google.com/)                                                                                                                                                                 |
| status                           | INTEGER |     -      | HTTP Archive crawl's response HTTP status                                                                                                                                                                                    |
| total_crawled_resources          | INTEGER |     -      | Total count of HTTP Archive crawl's request & response pairs (counts domain, subdomain and specific page crawls for every crawled domains)                                                                                   |
| total_crawled_domains            | INTEGER |     -      | Total count of HTTP Archive crawl's request & response pairs (counts only unique domains, applying condition `WHERE firstReq = true`)                                                                                        |
| nel_max_age                      | STRING  |     -      | NEL field: `max_age`                                                                                                                                                                                                         |
| nel_failure_fraction             | STRING  |   '1.0'    | NEL field: `failure_fraction`                                                                                                                                                                                                |
| nel_success_fraction             | STRING  |   '0.0'    | NEL field: `success_fraction`                                                                                                                                                                                                |
| nel_include_subdomains           | STRING  |  'false'   | NEL field: `include_subdomains`                                                                                                                                                                                              |
| nel_report_to                    | STRING  | 'default'  | NEL field: `report_to`                                                                                                                                                                                                       |
| total_crawled_resources_with_nel | INTEGER |     -      | Total count of responses that returned with NEL header contained in it's headers (counts every occurrence in monthly data, but the returned data itself contains only those responses that also contained Report-To headers) |
| rt_group                         | STRING  | 'default'  | Report-To field: `group`                                                                                                                                                                                                     |
| rt_collectors                    | STRING  |     -      | Report-To, all field values: `endpoints.url[]`                                                                                                                                                                               |

## Semantics

TODO how data must be filled out regarding correct NEL deployment constraints

TODO with this, find and filter out incorrectly set values before `Analyze` stage

1. Every row in the data represents a request-response pair. The requested (crawled) endpoint is not always the domain
   itself.
   A row can be a request to a unique DOMAIN, SUB-DOMAIN of that DOMAIN or a SUB-PAGE of that domain (
   using `https://domain/sub/page/path?query=string`).
   Each row in the nel data therefore **DOES NOT represent a unique domain**.

2. Every requests-response pair in the data must be using protocol HTTPS. NEL only works over HTTPS, not HTTP.

## Custom - Metrics

### Previous analysis metrics (base)

1. Unique domains queried within each year and those responding with valid NEL headers

    - RESULTS: Year; eTLD+1 Domain; NEL Count; NEL Percentage (% out of all eTLD+1's that returned response containing
      NEL)

    1. Take a month from the nel data
    2. Extract eTLD+1 domain from the urls for each URL in month data
    3. Group data by eTLD+1 domains (aggregate ETLD_PLUS_1_CONTAINS_NEL by CONTAINS_NEL - if any response contained
       valid nel - 1, otherwise 0)
    4. Sum up and RETURN the aggregated CONTAINS_NEL value
    5. Repeat for all other months and accumulate the ETLD_PLUS_1_CONTAINS_NEL by every year

    - ADDITIONAL: NEL Usage graph over the years


2. The count of NEL collector providers, the top four NEL collector providers for each analyzed year, and their share
   over the analyzed period
    1. Take a month from the nel data
    2. Extract all RT_ENDPOINTS from every request
    3. OUT_1 = count unique RT_ENDPOINTS
    4. OUT_2 = sort all RT_ENDPOINTS by the count of domains they were employed by
    5. OUT_3 = calculate the percentage of the total collector usage for each RT_ENDPOINT
    6. Repeat for all other months and update OUT_1 - OUT_3 data for analysed years


3. The number of NEL collector providers that are employed by a given number of domains (Number of collectors employed
   by 1, 2, 3-10, 11-100, 101-1K and more domains).
    1. Take a month from the nel data
    2. Extract all RT_ENDPOINTS from every request
    3. Count RT_ENDPOINT usage for each request
    4. Divide RT_ENDPOINTS into classes (of domain numbers) based on how many counted domains employ them
    5. Repeat for all other months and populate the domain employer classes with yearly data


4. NEL configuration over time
    1. TODO, but in essence - divide the 4 NEL configuration fields into classes of used value intervals and compute
       yearly distribution into those classes

### New analysis metrics (*can be derived from obtained results of base metrics)

1. The type of resource NEL is used for the most
    1. Take a month from the nel data
    2. Extract all TYPE values and sort them by count of their occurrence (as a NEL monitored resource type)
    3. Calculate the ratio of the total use for each TYPE
    4. Extract EXT values for the top N (top 10) used TYPEs (group by type) and find the most used EXT for every TYPE
    5. Calculate the ratio of the total use of the most used EXT for a specific TYPE


2. Famous / Popular companies using NEL
    - Use a list of popular domains to filter domains like these out
    - Or observe manually


3. Usage of NEL over time
    - In the aggregated set of NEL usage metric data, look for trends to describe (company started using, stopped, then
      after 2 years started again)


4. Trends emerging over the years
    - Preferred configuration field values during certain times
    - Configuration almost never used
    - Companies starting to use NEL in big numbers
    - Companies stopping to use NEL in big numbers


5. Domains eligible for real-time analysis
    1. Pick out some domains having a large number of subdomains and sub-pages crawled on HTTP Archive
    2. Run Selenium scripts on those and compare output