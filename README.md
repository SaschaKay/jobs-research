# English Speaking Data Specialists in Germany. Job Market Analytics.

A cloud-based data pipeline to monitor and analyze job postings in Germany. 
Dashboard 

Project uses external [API](https://rapidapi.com/techmap-io-techmap-io-default/api/daily-international-job-postings), which scrapes postings data from numerous portals.
For analysis, postings with 'Data' in the title were used. 

![image](https://github.com/user-attachments/assets/fc7f09e9-ae47-415c-8dab-9fbade713842)

## IaC
Set up with Terraform:
* Two VMs in Google Cloud(dev/prod)
* Cloud Storage Bucket
With Docker:
* Python container for processing and orchestrating
* Airflow containers: scheduler(+worker), webserver, postgress.
* In addition, stand-alone basic Airflow container is used to check all libraries versions installed on the jupyter container for compatibility with Airflow for further seamless migration.
Code and instructions for setting up infrastructure are in [job-research-infrastracture](https://github.com/SaschaKay/job-research-infrastracture) repository.

## Data Flow
Ingestion and transformation pipelines are realised in two different modules that launch automatically one after another, but can be relaunched independently, if needed.
#### Ingestion:
Loaded with DBT pipeline from custom paginated source to BQ. 
Source:
  1. Requests data with the DLT helper
  2. Immediately saves the response to Google Cloud Storage
  3. Partly flattens the response JSON, deletes duplicated fields, and hands it to DLT
DTL defines a schema based on data, handles datatypes, flattens JSON list fields into distinct tables, and loads them incrementally. 
#### Transformation:
Data loaded in Pandas DataFrames, processed, and downloaded to a distinct analytical dataset.
  1. Identical positions, posted on different portals, grouped based on location, preprocessed description, and title. Unique key for analytical table = id of position, generated with SHA1 based on preprocessed text fields.
  2. Locations and positions are standardized and normalized through aggregation from several low-quality fields and regular expressions.
  3. The final analytical table structure provides clean, ready-to-use, standardized data. Partitioned by year of the last post, created with this position.
  4. For the [dashboard in LookUp](https://lookerstudio.google.com/s/jqDeXhNYVhE), data is preaggregated in a view. Dashboard refreshes once per day.

![image](https://github.com/user-attachments/assets/59c57c42-4d0b-442f-a9df-27539204f520)

## Reproducibility
1. Key and instructions for API can be obtained [here](https://rapidapi.com/techmap-io-techmap-io-default/api/daily-international-job-postings)(there are 25 free requests available).
2. Set up infrastructure with [job-research-infrastracture](https://github.com/SaschaKay/job-research-infrastracture).
3. Clone this repository to your Airflow container in `/opt/airflow/repos`.
4. Create a link on DAGs files as described in `pipelines/dags/readme.txt`.
5. Create filr `/pipelines/rapidapi_jobs_posting/key.py` and add there
   
       API_KEY = "_your repidapi keey_"
7. Specify variable END_PAGE in `/pipelines/rapidapi_jobs_posting/config.py`. If not specified, all available data for DATE_CREATED period will be requested.
8. Change Google Cloud infrastructure parameters in GCS_PARAMS, BQ_PARAMS, and GCP_NAME variables.
9. Open Airflow Webserver UI. You should see jobs_postings_pipeline in the list of DUGs there. 
    ![image](https://github.com/user-attachments/assets/3688df19-af90-407c-b50b-68d2b153125d)
10. Trigger it with ![image](https://github.com/user-attachments/assets/d5b3a283-6fbc-4aa2-9ca1-c4bd52c3e47c)




   


