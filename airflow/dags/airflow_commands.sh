#!/bin/bash
airflow scheduler -D
airflow webserver -D
airflow dags unpause reddit_pipeline