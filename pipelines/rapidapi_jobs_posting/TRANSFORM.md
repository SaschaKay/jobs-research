# Data Cleaning Process

This document summarises how raw job postings are transformed into the analytical table. The full implementation lives in [pipeline_transform.py](./pipeline_transform.py).

## 1. Final table schema
The cleaned data is stored in the BigQuery table `jp.jobs`. The columns and their types are:

| Column | Type | Description | Source field(s) from API responce |
|-------|------|-------------|------------------------------------------|
| `id` | STRING | Unique job ID (same for the same postings on different platforms or with different post times) | Hash based on `title`, `company`, `description` |
| `date_created` | DATE | Day when the last post with this job was created | `date_created` |
| `company` | STRING | Hiring company name | `company` |
| `portal` | STRING | Source portal of the last post created | `portal` |
| `url` | STRING | Link to the last posting | `url` |
| `years_of_experience` | INT64 | Rounded years of experience required | `experience_requirements__months_of_experience` |
| `description` | STRING | Original job description | `description` |
| `city_clusters` | ARRAY<STRING> | Normalised location labels | Normalized `city` |
| `positions` | ARRAY<STRING> | Normalised job titles | Normalized `title` and `occupation` |
| `skills` | ARRAY<STRING> | Normalized skills, extracted from job's description | Extracted from `description` |

## 2. Raw field usage

| Raw field | Comment |
|-----------|-------------------------------------------|
| `title` | Raw titles; mapped to standard positions |
| `occupation` | Often too broad and not domain-specific (for example, 'Engineer' instead of 'Data Engineer' and 'Software Engineer'); combined with title for position mapping |
| `company` | Kept without changes |
| `city` | Chosen as a single reliable location field to define `city_clusters` |
| `url` | Reference link |
| `portal` | Originating portal |
| `description` | Source for skills |
| `experience_requirements__months_of_experience` | Converted to years and rounded |

## 3. Cleaning steps
Basic normalization and flattening are handled in the [load](./LOAD.md) part.
1. **Fetch new data** – query BigQuery for unprocessed loads with locale `en_DE`.
2. **Deduplicate postings** – compute a SHA‑1 hash of title, company, city, and description using [`get_post_id`](./functions.py) and keep only the most recent entry per hash.
3. **Normalise attributes** – apply `MappingRules` from [`mappings.py`](./mappings.py):
   - *Positions* – combine `title` and `occupation` and map to standard titles.
   - *City* – map `city` values to clusters, resolve ambiguous "Frankfurt" with `resolve_frankfurt_conflict`.
   - *Experience* – convert months to years and round up.
   - *Skills* – extract keywords from `description`; `link_skills_to_clouds` adds high‑level cloud labels.
4. **Load tables** – write normalised data to temporary tables and merge into `jp.jobs`.

## 4. Mapping rules
Mapping rules are maintained in Google Sheets for easy updating. The pipeline downloads them with `google_sheet_to_df` and passes them to `MappingRules`, which normalises the keywords and applies them to text fields. The spreadsheets are:

- [Positions mapping](https://docs.google.com/spreadsheets/d/1clAiWIVMD5bCJRHJr9-p2vw9h99W5sByAtqThIGREpo/edit?gid=1908800533#gid=1908800533)
- [City clusters mapping](https://docs.google.com/spreadsheets/d/1clAiWIVMD5bCJRHJr9-p2vw9h99W5sByAtqThIGREpo/edit?gid=217199346#gid=217199346)
- [Skills mapping](https://docs.google.com/spreadsheets/d/1clAiWIVMD5bCJRHJr9-p2vw9h99W5sByAtqThIGREpo/edit?gid=2082815936#gid=2082815936)

`MappingRules` groups rules by case‑ and space‑sensitivity, prepares them with `prepare_text`, and then searches the provided texts. For each job posting, the first matching rule is used when looking for a single value, or all matches are returned when building lists.
