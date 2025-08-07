# BigQuery datasets

|Dataset|Description|
|---------------|--------------------------------------------------------|
|jobs_postings|Technical and raw data layer. Includes DLT tables and transform pipeline logging tables.|
|jobs_postings_test|Analog of jobs_postings for dev enviroment.|
|jp|Cleaned and normalized data, processed by transform pipeline.|
|jp_test|Analog of jobs_postings for dev enviroment.|
|reports|Analytical tables, created specifically for the Looker dashboard. Use jp dataset as a source of data.|

# jobs_postings

## Naming

|Prefix|Description|
|-------------|----------------------------------------|
|_dlt|dlt logging tables|
|_jp|Pipelines logging and batch processing tables.|
|No prefix|Tables with processed data.|

## Tables

_in process...._

|Table|Description|
|-------------|----------------------------------------|
|`_dlt_loads`|dlt loads logging|
|`_dlt_pipeline_state`|dlt pipeline logging|
|`_dlt_version`|Tables schema logging (handled by dlt)|
|`jobs_posting`|Loadad data, updated by dbt. Includes unique key `_dlt_id` and link to `_dlt_loads` (`_dlt_loads`.`load_id` = `jobs_posting`.`_dlt_load_id`)|
|`jobs_posting__*`|Child dimention tables of `jobs_posting`, updated by dbt. Includes unique key `_dbt_id` and `_dlt_parent_id`(`jobs_posting` = `jobs_posting`.`_dlt_id`)|

# jp

_in process...._
