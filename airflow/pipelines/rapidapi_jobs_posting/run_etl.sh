#!/bin/bash

#To schedule:
#crontab -e
#0 10 * * * /home/jovyan/work/repos/jobs-research/pipelines/rapidapi_jobs_posting/run_etl.sh >> /home/jovyan/work/logs_jobs_postings 2>&1

BASE_DIR="$(dirname "$(realpath "$0")")"
LOG_DIR="/home/jovyan/work/logs_jobs_postings"
DATE=$(date +%F)

mkdir -p "$LOG_DIR"

MAIN_LOG="$LOG_DIR/etl_$DATE.log"
LOAD_LOG="$LOG_DIR/load_$DATE.log"
TRANSFORM_LOG="$LOG_DIR/transform_$DATE.log"

echo "===== ETL job started at $(date) =====" >> "$MAIN_LOG"
echo "===== ETL job started at $(date) =====" 

# Load step
echo "--- Running pipeline_load.py at $(date) ---" >> "$MAIN_LOG"
echo "--- Running pipeline_load.py at $(date) ---" >> "$LOAD_LOG"
python3 "$BASE_DIR/pipeline_load.py" >> "$LOAD_LOG" 2>&1
echo "pipeline_load.py finished at $(date)" >> "$MAIN_LOG"
echo "pipeline_load.py finished at $(date)" >> "$LOAD_LOG"

# Transform step
echo "--- Running pipeline_transform.py at $(date) ---" >> "$MAIN_LOG"
echo "--- Running pipeline_transform.py at $(date) ---" >> "$TRANSFORM_LOG"
python3 "$BASE_DIR/pipeline_transform.py" >> "$TRANSFORM_LOG" 2>&1
echo "pipeline_transform.py finished at $(date)" >> "$MAIN_LOG"
echo "pipeline_transform.py finished at $(date)" >> "$TRANSFORM_LOG"

echo "===== ETL job finished at $(date) =====" >> "$MAIN_LOG"
echo "===== ETL job finished at $(date) ====="
