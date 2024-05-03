# IBP-NEL-Analysis-Scripts
All the scripts to run the NEL technology deployment analysis and produce it's resulting reports.

## How to run

The instructions on how to configure each script before running it are stored inside the given script's documentation.

### HTTP Archive analysis

To run a complete analysis on HTTP Archive data:

1. configure and run the `query_and_store.py` script to obtain the complete analysis dataset
2. analyze downloaded data with `analyze_httparchive.py` script to compute the predefined metrics in `nel_analysis.py`
3. use the jupyter notebooks in the `./results` directory that start with a prefix of `httparchive_*` to visualize the metrics from the previous step

### Crawled data analysis

First, make sure the directory `./data` contains a file named `domains_to_crawl.parquet`, the input file for the crawler.
I provided the default one - a set of domains taken from HTTP Archive metric `nel_domain_resource_monitoring_stats` 
from April 2024 that were a part of the TOP 1M TRANCO domains & had at least 20 resources monitored by NEL.

##### Actualize the list of domains to crawl

To update these domains at a later date, either use `query_and_store.py` to download the latest available month data 
from HTTP Archive.
Then run the analysis script to compute the required metric and use the `select_domains_to_crawl.ipynb` notebook to read 
the domains from it.

Or, an alternative approach is to download domains from the Chrome User Experience Report dataset hosted
on BigQuery (named `chrome-ux-report`). 
This approach, however, does not have an implemented script among these to automate it.

The format for the input file mentioned is an Apache Parquet file with a pandas DataFrame that must contain an 
`url_domain` column with the domains to crawl. 

##### Run the crawl analysis

To run a complete analysis on crawled data:

1. configure and run the `crawl_and_store.py` script to crawl the pre-defined set of domains
2. analyze downloaded data with `analyze_crawled.py` script to compute the predefined metrics in `nel_analysis.py`
3. use the jupyter notebooks in the `./results` directory that start with a prefix of `crawled_*` to visualize the metrics from the previous step
