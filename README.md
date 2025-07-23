# English Speaking Data Specialists in Germany. 

# Goals
It is a personal pet-project, used to:
- collect relevant vacancies
- try new tools
- basic market research

The author so far has no plans to scale up this project, and sometimes sacrifices scalability for the sake of development speed. If you are planning to reuse part of the project or came from a LinkedIn link, ask me about bottlenecks :)

# Description
A cloud-based data pipeline to monitor and analyze job postings in Germany. 

Project uses external [API](https://rapidapi.com/techmap-io-techmap-io-default/api/daily-international-job-postings), which scrapes postings data from numerous portals.
For analysis, postings with 'Data' in the title were used. 

![image](https://github.com/user-attachments/assets/492003fe-ccb7-40af-ad3c-81ef673796aa)

## Links
* [Project's dashboard](https://lookerstudio.google.com/u/2/reporting/e029baeb-1698-40d5-8670-e279f4afe7c5/page/tEnnC/edit) _(in process...)_
* Infrastructure repository [job-research-infrastracture](https://github.com/SaschaKay/job-research-infrastracture).

## IaC
Set up with Terraform:
* Two VMs in Google Cloud(dev/prod)
* Cloud Storage Bucket
With Docker:
* Python container for processing and orchestrating
* Airflow containers: scheduler(+worker), webserver, postgress.
* In addition, a stand-alone basic Airflow container is used to check all library versions installed on the Jupyter container for compatibility with Airflow for further seamless migration.
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

# Reproducibility
1. Key and instructions for the API can be obtained [here](https://rapidapi.com/techmap-io-techmap-io-default/api/daily-international-job-postings) (25 free requests are available).
2. Set up infrastructure with [job-research-infrastracture](https://github.com/SaschaKay/job-research-infrastracture).
3. Clone this repository to your Airflow container in `/opt/airflow/repos`.
4. Create a link on DAGs files as described in `pipelines/dags/readme.txt`.
5. Create a file `/pipelines/rapidapi_jobs_posting/key.py` and add there
   
       API_KEY = "_your repidapi key_"
7. Specify variable END_PAGE in `/pipelines/rapidapi_jobs_posting/config.py`. If not specified, all available data for DATE_CREATED period will be requested.
8. Change Google Cloud infrastructure parameters in GCS_PARAMS, BQ_PARAMS, and GCP_NAME variables.
9. Open Airflow Webserver UI. You should see jobs_postings_pipeline in the list of DUGs there. 
    ![image](https://github.com/user-attachments/assets/3688df19-af90-407c-b50b-68d2b153125d)
10. Trigger it with ![image](https://github.com/user-attachments/assets/d5b3a283-6fbc-4aa2-9ca1-c4bd52c3e47c)

# To Do
1. Rewrite BigQuery helpers as a class.
2. Split transformation task in DAG into smaller parts.
3. Add analytics by key positions in Looker. 
4. Transfer views into DBT.
5. Create a database schema visualisation and checks in DBT.
6. Add an article with data cleaning details.
7. Break the data transform DAG into several sub-DAGs, with downloading intermediate results into BigQuery.
8. Add monitoring
+ unit tests
