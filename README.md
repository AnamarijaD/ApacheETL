
# Zoo data ETL

This project is about creating data pipeline using Apache Airflow to perform an ETL (Extract, Transform, Load)
process for zoo animal data.


## Features

 - Extract animal data from multiple CSV files.
 - Transform the data with various operations.
 - Aggregate and validate the data.
 - Load the final transformed and validated data into a new CSV file.
 - Upload DAG to Apache Airflow



## Deployment

To deploy this project run

```bash
  docker run -dit --name afs -v "$(pwd)/dags:/opt/airflow/dags" -v "$(pwd)/files:/opt/airflow/files" --rm -p 8080:8080 apache/airflow standalone
```
Getting password for user admin
```bash
  docker exec afs cat standalone_admin_password.txt
```

