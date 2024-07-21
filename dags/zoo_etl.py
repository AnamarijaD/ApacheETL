import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def extract_zoo_animals(**kwargs):
    '''
    Reads zoo animal data from a CSV file,
    and returns the data as a pandas DataFrame.
    '''
    df_animals = pd.read_csv('./files/zoo_animals.csv')
    kwargs['ti'].xcom_push(key='df_animals', value=df_animals.to_dict())


def extract_zoo_health_records(**kwargs):
    '''
    Reads zoo health records data from a CSV file,
    and returns the data as a pandas DataFrame.
    '''
    df_health = pd.read_csv('./files/zoo_health_records.csv')
    kwargs['ti'].xcom_push(key='df_health', value=df_health.to_dict())


def transform_data(**kwargs):
    '''
    This function takes two DataFrames,
    merges them, filters out animals younger than 2 years, converts animal names to title case,
    and ensures that health statuses are either "Healthy" or "Needs Attention".
    '''
    ti = kwargs['ti']
    df_animals = pd.DataFrame(ti.xcom_pull(task_ids='extract_zoo_animals', key='df_animals'))
    df_health = pd.DataFrame(ti.xcom_pull(task_ids='extract_zoo_health_records', key='df_health'))

    df = pd.merge(df_animals, df_health, on='animal_id')

    df = df[df['age'] >= 2]

    df['animal_name'] = df['animal_name'].str.title()

    df = df[df['health_status'].isin(['Healthy', 'Needs Attention'])]

    kwargs['ti'].xcom_push(key='transformed_data', value=df.to_dict())


def aggregate_data(**kwargs):
    ti = kwargs['ti']
    df = pd.DataFrame(ti.xcom_pull(task_ids='transform_data', key='transformed_data'))

    species_count = df.groupby('species').size().reset_index(name='species_count')
    health_status_count = df.pivot_table(index='species', columns='health_status', aggfunc='size', fill_value=0).reset_index()
    
    # Merge the species count with the health status counts
    result_df = species_count.merge(health_status_count, on='species')

    kwargs['ti'].xcom_push(key='aggregated_data', value=result_df.to_dict())


def validate_data(**kwargs):
    ti = kwargs['ti']
    df = pd.DataFrame(ti.xcom_pull(task_ids='aggregate_data', key='aggregated_data'))

    for _, row in df.iterrows():
        total_health_status_count = row['Healthy'] + row['Needs Attention']
        if row['species_count'] != total_health_status_count:
            print(f"Error: Counts do not match for species {row['species']}.")
            print(f"species_count: {row['species_count']}, total_health_status_count: {total_health_status_count}")

    kwargs['ti'].xcom_push(key='validated_data', value=df.to_dict())


def load_final_data(**kwargs):
    '''
    This function saves the final dataset into CSV file.
    '''
    ti = kwargs['ti']
    df = pd.DataFrame(ti.xcom_pull(task_ids='validate_data', key='validated_data'))
    df.to_csv('./files/final_zoo_data.csv', index=False)


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
    }


with DAG(
    'zoo_etl',
    default_args=default_args,
    description='ETL pipeline for zoo animal data',
    schedule_interval='*/5 * * * *',
) as dag:   

    extract_animals_task = PythonOperator(
        task_id='extract_zoo_animals',
        python_callable=extract_zoo_animals,
        provide_context=True
    )

    extract_health_task = PythonOperator(
        task_id='extract_zoo_health_records',
        python_callable=extract_zoo_health_records,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    aggregate_task = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        provide_context=True
    )

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_final_data',
        python_callable=load_final_data,
        provide_context=True
    )


    extract_animals_task >> extract_health_task >> transform_task >> aggregate_task >> validate_task >> load_task
