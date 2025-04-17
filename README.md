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
* In addition, some setup for Airflow was made(not used in this project, scheduled with cron). All libraries versions installed on the container are automatically checked for compatibility with Airflow for further seamless migration.
Code and instructions for setting up infrastructure are in [job-research-infrastracture](https://github.com/SaschaKay/job-research-infrastracture) repository.

## Data Flow
Ingestion and transformation pipelines are realised in two different modules that launch automatically one after another, but can be relaunched independently, if needed.
#### Ingestion:
Loaded with DBT pipeline from custom paginated source to BQ. 
Source:
  1. Requests data with the DLT helper
  2. Immediately saves response to Google Cloud Storage
  3. Partly flattens the response JSON, deletes duplicated fields, and hands it to DLT
DTL defines schema based on data, handles datatypes, flattens JSON list fields into distinct tables, and loads them incrementally. 
#### Transformation:
Data loaded in Pandas DataFrames, processed, and downloaded to the distinct analytical dataset.
  1. Identical positions, posted on different portals, grouped based on location, preprocessed description, and title. Unique key for analytical table = id of position, generated with SHA1 based on preprocessed text fields.
  2. Locations and positions standardized and normalized through aggregation from several low-quality fields and regular expressions.
  3. The final analytical table structure provides clean, ready-to-use, standardized data. Partitioned by year of the last post, created with this position.
  4. For the [dashboard in LookUp](https://lookerstudio.google.com/s/jqDeXhNYVhE), data is preaggregated in a view. Dashboard refreshes once per day.

![image](https://github.com/user-attachments/assets/59c57c42-4d0b-442f-a9df-27539204f520)

## Reproducibility
1. Key and instructions for API can be obtained [here](https://rapidapi.com/techmap-io-techmap-io-default/api/daily-international-job-postings)(there are 25 free requests available).
2. Set up infrastructure with [job-research-infrastracture](https://github.com/SaschaKay/job-research-infrastracture).
3. Clone this repository to your Jupyter container.
4. Pass your API key to the variable HEADERS["x-rapidapi-key"] in `/pipelines/rapidapi_jobs_posting/config.py`.
5. Note variable END_PAGE, if not specified, all available data for DATE_CREATED period will be requested.
6. Change Google Cloud infrastructure parameters in GCS_PARAMS, BQ_PARAMS, and GCP_NAME variables.
7. Specify your desired LOG_DIR in `/pipelines/rapidapi_jobs_posting/run_etl.sh`
8. Run run_etl.sh in terminal with

        bash run_etl.sh
9. To schedule, follow the instructions inside.


   


