# DATA PIPELINE PROJECT

Project to implemement data pipeline using AWS

## Objectives

- Uploads files from AWS based on date - including  any unprocessed files from previous batch
- Combine Files by date
- Transform files by adding additional column
- Load file to AWS output bucket
- save list of processed files to meta data

## Processing

Processing will check for unprocessed files in previous run by using meta data and process them.

A date comparison is performed