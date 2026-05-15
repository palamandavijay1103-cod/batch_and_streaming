FROM apache/airflow:2.9.1

USER root

RUN apt-get update && apt-get install -y default-jdk

USER airflow

COPY requirements.txt .

RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt