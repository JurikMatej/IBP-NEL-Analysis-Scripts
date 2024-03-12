# IBP-NEL-Analysis-Scripts
Scripts to run the NEL technology deployment analysis and produce it's resulting report

Everything here is a lie. Do NOT trust this readme!
TODO rewrite :)

## Usage

The environment used to run these scripts is powered by conda.
Use the following commands to set up the necessary environment.

```shell
# Conda set up
conda env create -n <ENVNAME> --file IBP-NEL-Analysis-Scripts.yml
conda activate <ENVNAME>

# Environment variables set up
cp .env_example .env # And then fill out the env variables with real data
```

## TODO

1. HTTP Archive
   1. query and store
   2. read filter merge save-new
   3. read analyze visualize

2. Selenium
   1. crawl and store (same format as 1.2.)
   2. read analyze visualize 