# Data-Lake-nanodegree

This repo contains all files for the fourth project of "Data Engineering Nanodegree" course from Udacity. See the project description below.

# Summary

A music streaming startup called Sparkify wants to migrate its data warehouse to a data lake.

This project aims to build an ETL capable of extracting data from S3, processing on a Spark cluster em save on S3 (Star Schema).

# Output data schema

### Fact table

* **songplays**: records in log data associated with song plays.

### Dimension tabels

* **users**: app users info.
* **songs**: songs info.
* **artists**: artists info.
* **time**: timestamps of records in songplays broken down into specific time units (hour, day, week, month, year, weekday).

# Files description

## etl.py

* Start a Spark connection
* Download data from S3 on Spark cluster
* Process data on Spark
* Save computed data on S3

## dl.cfg

* AWS credentials to EMR cluster and S3 (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY).