FROM apache/airflow:2.5.1

USER root

RUN apt-get update \
    && apt-get install -y libpq-dev gcc
    
USER airflow

RUN pip install --no-cache-dir psycopg2
RUN pip install --no-cache-dir ShopifyAPI
RUN pip install --no-cache-dir pandas
RUN pip install --no-cache-dir SQLAlchemy