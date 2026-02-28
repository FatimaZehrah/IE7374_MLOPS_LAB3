# **ETL Pipeline Lab: Google Cloud Storage to BigQuery using Cloud Run Functions**

## **Overview**
This lab implements an automated ETL pipeline on Google Cloud. When a newline-delimited JSON (`.ndjson`) file is uploaded to a Google Cloud Storage bucket, a Cloud Run function is triggered automatically. The function validates the file, routes it to the correct BigQuery table, loads the data, logs the outcome in an audit table, and archives the processed file.

This version of the lab is **my own implementation** and extends the original lab by using a different dataset, adding configuration-driven routing, creating an audit log, handling rejected files, and improving archive behavior.

---

## **Objective**
The goal of this lab is to build and verify an event-driven data pipeline that:

- listens for file uploads in a GCS bucket  
- triggers a Python cloud function automatically  
- loads data into BigQuery  
- documents the pipeline clearly for re-running  
- includes custom enhancements instead of copying the original lab directly  

---

## **Dataset Used**
For this lab, I used the **Google Analytics Sample dataset** from BigQuery public datasets.

**Source used:**  
`bigquery-public-data.google_analytics_sample.ga_sessions_20170801`

I created a flattened sample table in my own BigQuery dataset and exported it as NDJSON for pipeline testing.

### **Sample Schema Used**
The processed sessions data includes the following fields:

- `session_date`
- `fullVisitorId`
- `visitId`
- `visitNumber`
- `visits`
- `pageviews`
- `transactions`
- `revenue_micros`
- `device_category`
- `country`
- `channelGrouping`

---

## **My Custom Changes ("New Factors")**
This lab is not a copy of the original version. I introduced the following changes:

### **1. Different Dataset**
Instead of using the original sample JSON approach, I used Google Analytics sample session data and built my own flattened NDJSON test file.

### **2. Configuration-Driven Routing**
The function uses a `config.yaml` file to define:
- **project ID**  
- **dataset ID**  
- **folder routing**  
- **archive and reject prefixes**  
- **partitioning and clustering settings**  

### **3. Audit Logging**
The pipeline writes execution details into a BigQuery audit table called:

`staging.load_audit`

This records:
- **file name**  
- **file path**  
- **target table**  
- **status** (`SUCCESS` or `REJECTED`)  
- **rows loaded**  
- **BigQuery job ID**  

### **4. File Validation and Rejection Logic**
The function validates uploaded files and rejects files that:
- are not `.ndjson`
- are not in the expected `incoming/<folder>/<file>` path

Rejected files are sent to a `rejected/` path.

### **5. Archive Handling Improvement**
Initially, moving files to `archive/` triggered the function again because the trigger was attached to the whole bucket. I improved the function by adding logic to ignore files already inside:
- `archive/`
- `rejected/`

This prevented unnecessary reprocessing.

### **6. Partitioning and Clustering**
The destination BigQuery table is created with:
- **partitioning** on `session_date`
- **clustering** on `device_category` and `country`

This makes the implementation more production-like than the original lab.

---

## **Architecture**
The pipeline flow is:

1. A `.ndjson` file is uploaded into `incoming/sessions/` in the GCS bucket  
2. Cloud Storage emits an object finalized event  
3. Cloud Run function `ga-sessions-loader` is triggered  
4. The function:
   - reads the configuration and schema files  
   - validates the file path and extension  
   - determines the target BigQuery table  
   - creates the table if needed  
   - loads the data into BigQuery  
   - writes an audit row  
   - moves the processed file to `archive/`  
5. Files that do not match the expected rules are moved to `rejected/`

---

## **Technologies Used**
- **Google Cloud Storage**  
- **BigQuery**  
- **Cloud Run Functions**  
- **Eventarc**  
- **Python 3.11**  
- **GitHub**  

---

## **Repository Structure**
`src/` contains the cloud function source files.

- **`main.py`** → main function logic  
- **`requirements.txt`** → Python dependencies  
- **`config.yaml`** → pipeline configuration  
- **`schemas.yaml`** → BigQuery schema definition  

Other folders:
- **`sample_data/`** → local test/sample files  
- **`docs/`** → optional supporting notes  
- **`README.md`** → lab documentation  

---

## **Google Cloud Resources Used**
- **Project ID:** `gcp-lab1-fatimazehrah`  
- **BigQuery dataset:** `staging`  
- **Cloud Storage bucket:** `gcp-lab1-fatimazehrah-lab-bucket`  
- **Cloud Run function:** `ga-sessions-loader`  

---

## **Setup and Implementation Steps**

### **Step 1: Create a Google Cloud Project**
Create a GCP project and make sure billing is enabled.

### **Step 2: Enable Required APIs**
Enable the following APIs:
- **BigQuery API**  
- **Cloud Storage API**  
- **Cloud Functions / Cloud Run Functions support**  
- **Eventarc API**  
- **Cloud Build API**  
- **Artifact Registry API**  
- **Cloud Run Admin API**  

### **Step 3: Create the BigQuery Dataset**
Create a dataset named:

`staging`

### **Step 4: Create the Cloud Storage Bucket**
Create a bucket in the **US multi-region**.

Bucket used in this lab:

`gcp-lab1-fatimazehrah-lab-bucket`

### **Step 5: Prepare Sample Data in BigQuery**
Run this query in BigQuery to create a flattened sample table from the public Google Analytics dataset:

```sql
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
