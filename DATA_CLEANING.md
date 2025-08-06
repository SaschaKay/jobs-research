# Data Cleaning Process

This document summarises how raw job postings are transformed into the analytical table. The full implementation lives in [pipeline_transform.py](./pipelines/rapidapi_jobs_posting/pipeline_transform.py).

## 1. Final table schema
The cleaned data is stored in BigQuery table `jp.jobs`. The columns and their types are:

| Column | Type | Description |
|-------|------|-------------|
| `id` | STRING | Unique hash of title, company, city and description |
| `date_created` | DATE | Day when the post was first seen |
| `company` | STRING | Hiring company name |
| `portal` | STRING | Source portal |
| `url` | STRING | Link to the original posting |
| `years_of_experience` | INT64 | Rounded years of experience required |
| `description` | STRING | Original job description |
| `city_clusters` | ARRAY<STRING> | Normalised location labels |
| `positions` | ARRAY<STRING> | Normalised job titles |
| `skills` | ARRAY<STRING> | Extracted skills |

## 2. Raw field usage

| Raw field | Used in final dataset? | Reason |
|-----------|-----------------------|-------|
| `title` | ✓ via `positions` | Raw titles like "Engineer" were too broad; mapped to standard positions |
| `occupation` | ✓ via `positions` | Combined with title for position mapping |
| `company` | ✓ | Kept as company identifier |
| `city` | ✓ via `city_clusters` | Chosen as single reliable location field |
| `url` | ✓ | Reference link |
| `portal` | ✓ | Originating portal |
| `description` | ✓ | Source for skills and job context |
| `experience_requirements__months_of_experience` | ✓ as `years_of_experience` | Converted and rounded up |
| `hiring_organization__name` | ✗ | Duplicates `company` |
| `hiring_organization__url` | ✗ | Often missing; duplicates company info |
| `source` | ✗ | Same as `portal` |
| `employment_type` | ✗ | Inconsistent values |
| `base_salary__value__min_value`/`max_value` | ✗ | Missing units and rarely filled |
| `job_location__identifier` | ✗ | Too sparse and inconsistent |
| `applicant_location_requirements` | ✗ | Mostly missing |
| `timezone_offset` | ✗ | Nearly constant |

## 3. Cleaning steps
1. **Fetch new data** – query BigQuery for unprocessed loads with locale `en_DE`.
2. **Deduplicate postings** – compute a SHA‑1 hash of title, company, city and description using [`get_post_id`](./pipelines/rapidapi_jobs_posting/functions.py) and keep only the most recent entry per hash.
3. **Normalise attributes** – apply `MappingRules` from [`mappings.py`](./pipelines/rapidapi_jobs_posting/mappings.py):
   - *Positions* – combine `title` and `occupation` and map to standard titles.
   - *City* – map `city` values to clusters, resolve ambiguous "Frankfurt" with `resolve_frankfurt_conflict`.
   - *Experience* – convert months to years and round up.
   - *Skills* – extract keywords from `description`; `link_skills_to_clouds` adds high‑level cloud labels.
4. **Load tables** – write normalised data to temporary tables and merge into `jp.jobs` using [`BQHelper`](./common/bq_helper.py).

## 4. Mapping rules
Mapping rules are maintained in Google Sheets so non‑developers can update them. The pipeline downloads them with `google_sheet_to_df` and passes them to `MappingRules`, which normalises the keywords and applies them to text fields. The spreadsheets are:

- [Positions mapping](https://docs.google.com/spreadsheets/d/1clAiWIVMD5bCJRHJr9-p2vw9h99W5sByAtqThIGREpo/edit?gid=1908800533#gid=1908800533)
- [City clusters mapping](https://docs.google.com/spreadsheets/d/1clAiWIVMD5bCJRHJr9-p2vw9h99W5sByAtqThIGREpo/edit?gid=217199346#gid=217199346)
- [Skills mapping](https://docs.google.com/spreadsheets/d/1clAiWIVMD5bCJRHJr9-p2vw9h99W5sByAtqThIGREpo/edit?gid=2082815936#gid=2082815936)

`MappingRules` groups rules by case‑ and space‑sensitivity, prepares them with `prepare_text`, and then searches the provided texts. For each job posting, the first matching rule is used when looking for a single value or all matches are returned when building lists.
