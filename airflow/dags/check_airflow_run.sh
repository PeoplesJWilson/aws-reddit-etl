#!/bin/bash
airflow dags list-runs -d reddit_pipeline
python3 test_insertion.py
