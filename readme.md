Create .env in the project root:
---------
SYMBOLS=MSFT,AAPL
ALPHA_VANTAGE_KEY=YOUR_API_KEY_HERE

POSTGRES_HOST=db
POSTGRES_PORT=5432
POSTGRES_DB=db
POSTGRES_USER=db_user
POSTGRES_PASSWORD=db_password
export ROOT_DIR=#ADD YOUR PROJECTPATH

start the pipeline
--------
    docker compose up -d

Access Airflow using
http://localhost:8000

Airflow prints your admin password on start:

Trigger the Pipeline
-------------
    Inside the Airflow UI:
    Find stockdata_api_orch
    Turn it ON
    Click Trigger DAG
    This runs:
    ingestdata_task → Fetch & load stock data
    transform_data_task → Execute DBT transformations


Check the Data in PostgreSQL
-------------
Start SQL shell:

docker exec -it postgres_container psql -U db_user -d db

select record
SELECT * FROM dev.staging;

View Latest Records
SELECT *
FROM dev.staging
ORDER BY timestamp DESC
LIMIT 10;
