#!/bin/bash
sudo apt-get update
sudo apt install python3-pip
sudo pip install apache-airflow
sudo pip install mysql-connector-python
sudo pip install requests
sudo pip install pendulum
mkdir /home/ubuntu/airflow
mkdir /home/ubuntu/airflow/dags
cd /home/ubuntu/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db init