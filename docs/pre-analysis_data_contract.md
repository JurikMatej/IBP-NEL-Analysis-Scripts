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
response pairs (with additional metadata) in which the response **contained both `NEL` and `Report-To`** HTTP response headers.
That is, analysis data **only contains correctly functional NEL** deployment instances.

NEL deployment is correctly functional only when HTTP response from the target web server contains a valid `NEL` and 
a valid `Report-To` headers with matching `NEL.report-to` and `Report-To.group` HTTP header values. 

Abbreviation Legend:

| Abbrev. | Full name             |
|:--------|:----------------------|
| nel     | Network Error Logging |
| rt      | Report To             |

### Schema

| Key                     |  Type   | Default To | Description                                                                                                                                                                                        |
|:------------------------|:-------:|:----------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| requestId               | INTEGER |     -      | Primary key - The particular HTTP Archive crawl request's ID                                                                                                                                       |
| firstReq                |  BOOL   |     -      | Flag indicating whether that request was the first request made to the domain - {url} field                                                                                                        |
| type                    | STRING  |     -      | Requested resource type (html, script, video, font)                                                                                                                                                |
| ext                     | STRING  |     -      | Requested resource file extension without the leading dot (html, json, mp4, woff2)                                                                                                                 |
| url                     | STRING  |     -      | HTTP Archive crawl's requested URL (https://www.google.com/)                                                                                                                                       |
| status                  | INTEGER |     -      | HTTP Archive crawl's response HTTP status                                                                                                                                                          |
| total_crawled_resources | INTEGER |     -      | Total count of HTTP Archive crawl's request & response pairs (counts domain, subdomain and specific page crawls for every crawled domains)                                                         |
| total_crawled_domains   | INTEGER |     -      | Total count of HTTP Archive crawl's request & response pairs (counts only unique domains, applying condition `WHERE firstReq = true`)                                                              |
| nel_max_age             | STRING  |     -      | NEL field: `max_age`                                                                                                                                                                               |
| nel_failure_fraction    | STRING  |   '1.0'    | NEL field: `failure_fraction`                                                                                                                                                                      |
| nel_success_fraction    | STRING  |   '0.0'    | NEL field: `success_fraction`                                                                                                                                                                      |
| nel_include_subdomains  | STRING  |  'false'   | NEL field: `include_subdomains`                                                                                                                                                                    |
| nel_report_to_group     | STRING  | 'default'  | NEL field: `report_to`                                                                                                                                                                             |
| total_nel_count         | STRING  |     -      | Total count of responses that returned with NEL header contained in it's headers (counts every occurrence, but the data itself contain only those responses that also contained Report-To headers) |
| rt_group                | STRING  | 'default'  | Report-To field: `group`                                                                                                                                                                           |
| rt_endpoints            | STRING  |     -      | Report-To, all field values: `endpoints.url`                                                                                                                                                       |
| rt_url                  | STRING  |     -      | Report-To, first field value: `endpoints.url` ?????????????????????????? NOT SO SURE PLS CHECK                                                                                                     |


## Semantics

TODO how data must be filled out regarding correct NEL deployment constraints 

TODO with this, find and filter out incorrectly set values before `Analyze` stage

## Custom - Metrics

TODO define the analyzed metrics here as a custom addition to the data contracts
     
TODO structure = declarative approach (how to get the resulting metric by applying operations on the data defined in the schema)

TODO required - previously analyzed

TODO optional - my proposals