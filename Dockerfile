# 1. Base Image: Use official Apache Airflow with Python 3.10
FROM apache/airflow:2.7.1-python3.10

USER root

# 2. System Dependencies: Install Java (required for PySpark) and Postgres dev libraries
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    libpq-dev gcc python3-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# 3. Python Dependencies: Install Airflow providers, PySpark, and dbt ecosystem
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    apache-airflow-providers-amazon \
    apache-airflow-providers-postgres \
    pandas \
    requests \
    dbt-core==1.11.7 \
    dbt-postgres==1.10.0 \
    dbt-redshift

# 4. dbt Project Setup: Copy dbt source code and set the profiles directory environment variable
COPY --chown=airflow:root ./crypto_dbt /opt/airflow/crypto_dbt
ENV DBT_PROFILES_DIR=/opt/airflow/crypto_dbt

# 5. Initialization: Set working directory and pre-install dbt packages/dependencies
WORKDIR /opt/airflow/crypto_dbt
RUN dbt deps