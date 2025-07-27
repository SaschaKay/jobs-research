# Scaling the Jobs Research Project

This project was built as a lightweight prototype to collect and transform job postings. If it needs to process significantly more data, consider the following options.


## Airflow Deployment

* For any scaling splitting the Airflow scheduler and worker to dedicated containers or VMs is strongly recommended. Worker container resources should be limited.
* In additional, this allows the worker nodes to scale independently from the scheduler. Consider horizontal scaling for the worker container if tasks take long or a big backlog of DAG runs is expected. Note that load task can not be paralleled due to requests frequency limit. Load and transform tasks are independent can be spitted into two dags without significant refinements.


## Load task scaling

Note that the excising load job should not be executed in parallel because of the **request limits** (check specification of your tariff it the API documentation). For parallel or distributed processing load all data first and then transform in parallel.

Right now the load job uses custom paginated source that 
* saves raw responses in GS,
* performs simple transformations necessary for dlt to handle loading structured data in the BQ.
This implementation allows skip one extra read from GS but doesn't allow parallel dlt job because of the requests limit. If needed, leave only saving data step in the load task. dlt allows natural parallel processing, you can still use it for transferring data from GS to BQ. Note that dlt can handle big amount of data naturally and execution time will not grow linearly with the amount of data. dlt need some sugnificant amount of time to check and verify the schema, avoid paralleling dlt loads in small batches by hand instead of using inbuilt tools.


## Transformation scaling scenarios

RN data cleaning and normalization done in pandas. While pandas can be implemented for parallel or distributed processing, consider changing the tool for more natural big data processing, if needed.

### Quick Fix - Incremental Processing

The ingestion pipeline already supports specifying `start_page` and `end_page` in `pipeline/rapidapi_jobs_posting/config.py`. Right now all available pages requested by once, and this parameters only used for testing or downloading the rest of the pages in case of job failing after receiving only the part of available data. While scaling this parameters allows quickly implement splitting large date ranges into smaller batches for incremental processing. For this set function get_end_page (currently the part of a load task) as an independent task or dag and then trigger rapidapi_jobs_posting_dag repeatedly with different `start_page` and `end_page` until you reach the last page.
While being slow, this approach allows quickly prevent failing dags by resources, while avoiding scaling VM infrastructure (and paying for more resources here) and can be used for scaling up to some point.


### Get off Easy - Maximize transformations in BigQuery 

Part of the transformation logic may be easily transferred from pandas into SQL scripts while leaving only complicated mapping logic in pandas (incremental, or paralleled, if needed). Do not to parallel requests for staying withing available requests frequency limits.


### Robust - Distributed processing

* When a single machine is not enough, consider using a distributed engine, such as **Spark or Dask** instead of pandas. It can handle larger volumes and integrate with BigQuery or Cloud Storage. It also allows avoid costly upscaling of an permanent worker VM.
* For fully managed execution, **Apache Beam** can be an alternative to run Python pipelines at scale. 
* Requests part should be managed independently for staying withing available requests frequency limits.


## BigQuery Tables Optimization

As the dataset grows, **add partitioning**.
* Add partition by post **date** or ingestion timestamp. Depending on the dataset size it might be days, weeks, month, or years. If using dlt, add partition by data in not only in **analytical**, but also in **dlt** preloaded tables. In **transformation** tasks  use it as **filter** in combination with load_id to fetch new data.
* Consider adding partitioning by most popular positions or locations, depending on your needs.

The rule of the thumb while experimenting with partitions - typical size of a partition shouldn't be less than 100 Mb.


## Further Recommendations

* Monitor API usage and set alerts on the ingestion DAG so you do not hit rate limits.
* Track transformation runtime and output size to detect when distributed engine becomes necessary.