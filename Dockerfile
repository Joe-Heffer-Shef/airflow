FROM apache/airflow:2.0.2
USER root

# Install extra Python packages
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

USER airflow
