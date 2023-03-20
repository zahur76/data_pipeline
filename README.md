# DATA PIPELINE PROJECT

Project to implemement data pipeline using AWS

## Objectives

- Uploads files from AWS based on date - including  any unprocessed files from previous batch
- Combine Files by date
- Transform files by adding additional column
- Load file to AWS output bucket
- save list of processed files to meta data

## Data Processing

Before any run meta data will be used to determine which folders need processing. This will cater for scenario in which run has failed by scheduler

1. List all folders in data input bucket
2. load meta file to check which folders have been processed
3. transform and load only those data which have not been processes (1-2) from chosen date and before

A date comparison is performed

## Test
- unittest
- integration test

* Command
- coverage run --omit=*/.virtualens/*,*/test/* -m unittest discover -v
- coverage html/report - generate report
