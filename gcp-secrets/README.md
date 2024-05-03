## Google Cloud Platform - Service Secrets

Setup consists of:

1. creating a Google Cloud project
2. creating a service account for that project (admin privileges for BigQuery and Storage)
3. creating a json key for that project
4. and storing it here in this directory

Then, use the name of that json key to rewrite the path to secrets file in `query_and_store.py` script.

For an example of the result json key's content structure, see `example.json` in this directory. 