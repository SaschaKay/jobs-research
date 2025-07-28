# Scaling the Jobs Research Project

This project was built as a lightweight prototype to collect and transform job postings. If it needs to process significantly more data, consider the following options.


## Airflow Deployment

* When scaling, deploy the Airflow scheduler and workers on separate containers or VMs. Limit worker resources to prevent task concurrency from overwhelming the scheduler.
* Additionally, this separation allows worker nodes to scale independently from the scheduler. Note that, although manual horizontal scaling for workers is possible, it is preferable to use autoscaled cloud-native instruments instead (more details in the Distributed Processing section). 
* Note that the load task cannot be parallelized because of request limits. Load and transform tasks are independent and can be split into two DAGs without significant refinements.


## Load task scaling

Note that the existing load job should not be executed in parallel because of **request limits** (check the limits of your tariff in the API documentation). For parallel or distributed processing, load all data first and then transform in parallel.
As data volume grows, track the last processed page or timestamp so that loads can resume from failures without duplicating requests.

Right now, the load job uses a custom paginated source that:
* saves raw responses in Cloud Storage,
* performs simple transformations necessary for dlt to load structured data into BigQuery.
This implementation skips one extra read from Cloud Storage but prevents running the dlt job in parallel because of the request limit. If needed, keep only the data-saving step in the load task. dlt enables natural parallel processing, allowing you to continue using it to transfer data from Cloud Storage to BigQuery. Note that dlt can handle large volumes natively, and execution time will not grow linearly with data size. However, dlt needs a significant amount of time to verify schemas—avoid parallelizing dlt loads in small batches by hand when built-in tools provide better performance.


## Transformation scaling scenarios

Currently, data cleaning and normalization are performed using pandas. While pandas can be implemented for parallel or distributed processing, consider switching to tools that handle big data more naturally if needed.

### Quick Fix - Incremental Processing

The ingestion pipeline already supports `start_page` and `end_page` in `pipeline/rapidapi_jobs_posting/config.py`. At the moment, all pages are requested at once, and these parameters are only used for testing or to resume a failed load. When scaling, use them to split large date ranges into smaller batches for incremental processing. Extract the `get_end_page` function (currently part of the load task) into a separate task or DAG, then trigger `rapidapi_jobs_posting_dag` repeatedly with different `start_page` and `end_page` values until all pages are processed. Although this approach is slower, it can prevent DAG failures without immediately scaling the VM infrastructure and works up to a certain point.


### Get Off Easy - Maximize Transformations in BigQuery

Move as much transformation logic as possible from pandas into SQL scripts, leaving only complex mapping in pandas (incremental or parallel if needed). Do not parallel requests to stay within available request limits.


### Robust - Distributed Processing

* When a single machine is not enough, consider a distributed engine for Python-specific logic instead. Transfer all SQL-like logic to BigQuery.
* For fully managed execution, run **Apache Beam** pipeline on **Cloud Dataflow**, which provides autoscaling and built-in monitoring. Airflow provides operators for submitting and monitoring Dataflow jobs. 
* Avoid running unmanaged Spark or Dask locally (will need costly VM scaling and manual DevOps management).
* Request handling should be managed independently to stay within available request limits.


## BigQuery Tables Optimization

As the dataset grows, **add partitioning**.
* Partition by post **date** or ingestion timestamp. Depending on the dataset size, this may be by day, week, month, or year. If using dlt, add partitions not only to the analytical tables but also to the dlt staging tables. In transformation tasks, use these partitions as filters, along with `load_id`, to fetch new data.
* Use BigQuery clustering together with partitions to speed up common queries. Consider clustering by the most popular positions or locations, depending on your needs.

A rule of thumb when experimenting with partitions is that each partition should be at least around **100 MB**. Start implementing clustering if your partition size is at least **256MB**.


## Further Recommendations

* Monitor API usage and set alerts on the ingestion DAG so you do not hit rate limits.
* Track transformation runtime, memory usage, and output size to detect when a distributed engine becomes necessary.
* Monitor BigQuery slot usage and billing to control costs as data volume grows.
