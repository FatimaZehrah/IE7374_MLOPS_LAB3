# IE7374_MLOPS_LAB3
ETL Pipeline Lab: Google Cloud Storage to BigQuery using Cloud Run Functions
Overview

This lab implements an automated ETL pipeline on Google Cloud. When a newline-delimited JSON (.ndjson) file is uploaded to a Google Cloud Storage bucket, a Cloud Run function is triggered automatically. The function validates the file, routes it to the correct BigQuery table, loads the data, logs the outcome in an audit table, and archives the processed file.

This version of the lab is my own implementation and extends the original lab by using a different dataset, adding configuration-driven routing, creating an audit log, handling rejected files, and improving archive behavior.

Objective

The goal of this lab is to build and verify an event-driven data pipeline that:

listens for file uploads in a GCS bucket

triggers a Python cloud function automatically

loads data into BigQuery

documents the pipeline clearly for re-running

includes custom enhancements instead of copying the original lab directly

Dataset Used

For this lab, I used the Google Analytics Sample dataset from BigQuery public datasets.

Source used:

bigquery-public-data.google_analytics_sample.ga_sessions_20170801

I created a flattened sample table in my own BigQuery dataset and exported it as NDJSON for pipeline testing.

Sample schema used

The processed sessions data includes the following fields:

session_date

fullVisitorId

visitId

visitNumber

visits

pageviews

transactions

revenue_micros

device_category

country

channelGrouping

My Custom Changes (“New Factors”)

This lab is not a copy of the original version. I introduced the following changes:

1. Different dataset

Instead of using the original sample JSON approach, I used Google Analytics sample session data and built my own flattened NDJSON test file.

2. Configuration-driven routing

The function uses a config.yaml file to define:

project ID

dataset ID

folder routing

archive and reject prefixes

partitioning and clustering settings

3. Audit logging

The pipeline writes execution details into a BigQuery audit table called:

staging.load_audit

This records:

file name

file path

target table

status (SUCCESS or REJECTED)

rows loaded

BigQuery job ID

4. File validation and rejection logic

The function validates uploaded files and rejects files that:

are not .ndjson

are not in the expected incoming/<folder>/<file> path

Rejected files are sent to a rejected/ path.

5. Archive handling improvement

Initially, moving files to archive/ triggered the function again because the trigger was attached to the whole bucket. I improved the function by adding logic to ignore files already inside:

archive/

rejected/

This prevented unnecessary reprocessing.

6. Partitioning and clustering

The destination BigQuery table is created with:

partitioning on session_date

clustering on device_category and country

This makes the implementation more production-like than the original lab.

Architecture

The pipeline flow is:

A .ndjson file is uploaded into incoming/sessions/ in the GCS bucket

Cloud Storage emits an object finalized event

Cloud Run function ga-sessions-loader is triggered

The function:

reads the configuration and schema files

validates the file path and extension

determines the target BigQuery table

creates the table if needed

loads the data into BigQuery

writes an audit row

moves the processed file to archive/

Files that do not match the expected rules are moved to rejected/

Technologies Used

Google Cloud Storage

BigQuery

Cloud Run Functions

Eventarc

Python 3.11

GitHub

Repository Structure

src/ contains the cloud function source files.

main.py → main function logic

requirements.txt → Python dependencies

config.yaml → pipeline configuration

schemas.yaml → BigQuery schema definition

Other folders:

sample_data/ → local test/sample files

docs/ → optional supporting notes

README.md → lab documentation

Google Cloud Resources Used

Project ID: gcp-lab1-fatimazehrah

BigQuery dataset: staging

Cloud Storage bucket: gcp-lab1-fatimazehrah-lab-bucket

Cloud Run function: ga-sessions-loader

Setup and Implementation Steps
Step 1: Create a Google Cloud project

Create a GCP project and make sure billing is enabled.

Step 2: Enable required APIs

Enable the following APIs:

BigQuery API

Cloud Storage API

Cloud Functions / Cloud Run Functions support

Eventarc API

Cloud Build API

Artifact Registry API

Cloud Run Admin API

Step 3: Create the BigQuery dataset

Create a dataset named:

staging

Step 4: Create the Cloud Storage bucket

Create a bucket in the US multi-region.

Bucket used in this lab:

gcp-lab1-fatimazehrah-lab-bucket

Step 5: Prepare sample data in BigQuery

Run this query in BigQuery to create a flattened sample table from the public Google Analytics dataset:

CREATE OR REPLACE TABLE `gcp-lab1-fatimazehrah.staging.ga_sessions_sample_20170801` AS
SELECT
  PARSE_DATE('%Y%m%d', date) AS session_date,
  fullVisitorId,
  visitId,
  visitNumber,
  totals.visits AS visits,
  totals.pageviews AS pageviews,
  totals.transactions AS transactions,
  totals.totalTransactionRevenue AS revenue_micros,
  device.deviceCategory AS device_category,
  geoNetwork.country AS country,
  channelGrouping
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
LIMIT 5000;

Step 6: Export the sample table to GCS as NDJSON

Export the table to:

gs://gcp-lab1-fatimazehrah-lab-bucket/incoming/sessions/ga_sessions_20170801.ndjson

Format:

Newline-delimited JSON

No compression

Step 7: Create the audit table

Create the audit table:
CREATE TABLE IF NOT EXISTS `gcp-lab1-fatimazehrah.staging.load_audit` (
  run_ts TIMESTAMP,
  file_name STRING,
  gcs_uri STRING,
  target_table STRING,
  status STRING,
  rejected_reason STRING,
  rows_loaded INT64,
  bq_job_id STRING
);
Step 8: Prepare the GitHub repository

Create a repository with:

README.md

.gitignore

src/main.py

src/requirements.txt

src/config.yaml

src/schemas.yaml

Step 9: Add source files

Add the source code and configuration files to the repository and also to the Cloud Run function source editor.

Step 10: Deploy the Cloud Run function

Create a Python Cloud Run function with:

Name: ga-sessions-loader

Region: us-central1

Runtime: Python 3.11

Entry point: hello_gcs

Step 11: Add the trigger

Attach an Eventarc / Cloud Storage trigger:

event provider: Cloud Storage

event type: google.cloud.storage.object.v1.finalized

bucket: gcp-lab1-fatimazehrah-lab-bucket

Step 12: Grant IAM roles

Grant the default compute service account these roles:

BigQuery Admin

Eventarc Event Receiver

Storage Object Admin

Step 13: Test the pipeline

Upload a file into:

incoming/sessions/

Example test filename:

ga_sessions_20170801_test3.ndjson

The expected behavior is:

file disappears from incoming/sessions/

file appears in archive/

BigQuery table is loaded

audit row is inserted

How to Re-run the Lab

To re-run this lab from scratch:

Ensure the GCP project, APIs, bucket, and dataset are available

Confirm the audit table exists

Confirm the Cloud Run function is deployed and active

Prepare or export a valid .ndjson file with the expected schema

Upload the file into:

incoming/sessions/

Wait 30–60 seconds

Check:

Cloud Run logs

BigQuery destination table

load_audit

archive/ folder in the bucket

Expected Outputs
BigQuery tables

staging.ga_sessions_sample_20170801

staging.ga_sessions

staging.load_audit

Bucket folders

incoming/sessions/

archive/

rejected/

Verification Performed

I verified the lab by:

confirming the exported NDJSON file existed in the bucket

deploying the function and successfully attaching the trigger

uploading new test files with unique names

observing that files were processed and moved from incoming/sessions/

confirming that staging.ga_sessions was populated

confirming that staging.load_audit contained SUCCESS rows

improving the function so archived files are ignored rather than reprocessed

Troubleshooting Notes
Issue 1: Event payload parsing error

At one point, the function failed because the incoming event body was being treated as bytes instead of JSON. I fixed this by safely parsing the request JSON and falling back to parsing raw request bytes if necessary.

Issue 2: Archive path re-triggering the function

Because the trigger listens to the whole bucket, moving files into archive/ created another object-finalized event. I fixed this by adding an early return for files that start with:

archive/

rejected/

Issue 3: Trigger permission propagation

The Eventarc trigger initially failed during setup. Retrying after the required permissions propagated resolved the issue.

Key Learning Outcomes

From this lab, I learned how to:

build an event-driven ETL workflow in GCP

export structured data into GCS as NDJSON

load data automatically into BigQuery

use configuration files to make cloud functions cleaner

implement validation, auditing, and archive/reject handling

debug Cloud Run function errors using logs

improve pipeline behavior after observing real trigger side effects

Final Result

This lab successfully demonstrates a working automated ETL pipeline from Google Cloud Storage to BigQuery using Cloud Run Functions, with additional improvements beyond the original lab specification.
