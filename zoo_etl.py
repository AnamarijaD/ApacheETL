import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'zoo_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for zoo animal data',
    schedule_interval=timedelta(days=1),
)

def extract_zoo_animals():
    '''
    Reads zoo animal data from a CSV file.

    This function reads the 'zoo_animals.csv' file located in the './files/' directory,
    and returns the data as a pandas DataFrame.
    '''
    df_animals = pd.read_csv('./files/zoo_animals.csv')
    return df_animals

def extract_zoo_health_records():
    '''
    Reads zoo health records data from a CSV file.

    This function reads the 'zoo_health_records.csv' file located in the './files/' directory,
    and returns the data as a pandas DataFrame.
    '''
    df_health = pd.read_csv('./files/zoo_health_records.csv')
    return df_health

def transform_data(df_animals, df_health):
    '''
    Transforms the extracted zoo data.

    This function takes two DataFrames, one for zoo animals and one for health records,
    merges them, filters out animals younger than 2 years, converts animal names to title case,
    and ensures that health statuses are either "Healthy" or "Needs Attention".
    The transformed data is then returned as a DataFrame.
    
    Args:
        df_animals (pd.DataFrame): DataFrame containing zoo animal data.
        df_health (pd.DataFrame): DataFrame containing zoo health records data.

    Returns:
        pd.DataFrame: Transformed DataFrame with filtered and formatted data.
    '''
    # Merge data on 'animal_id'
    df = pd.merge(df_animals, df_health, on='animal_id')

    # Filter out animals where age is less than 2 years
    df = df[df['age'] >= 2]

    # Convert 'animal_name' to title case
    df['animal_name'] = df['animal_name'].str.title()

    # Ensure 'health_status' contains only "Healthy" or "Needs Attention"
    df = df[df['health_status'].isin(['Healthy', 'Needs Attention'])]

    return df

def aggregate_and_validate():
    '''
    Aggregates and validates the transformed zoo data.

    This function reads the transformed zoo data, aggregates it to count the number of animals
    per species and the count of each health status. It then validates that no data is missing
    and saves the aggregated results to CSV files.

    Raises:
        ValueError: If any data is missing during validation.
    '''
    df = pd.read_csv('./files/transformed_zoo_data.csv')

    # Aggregate data
    species_count = df.groupby('species').size().reset_index(name='species_count')
    health_status_count = df['health_status'].value_counts().reset_index()
    health_status_count.columns = ['health_status', 'count']

    # Validate data
    if species_count.isnull().values.any() or health_status_count.isnull().values.any():
        raise ValueError("Validation failed: Missing data")

    # Save intermediate aggregated data
    species_count.to_csv('./files/species_count.csv', index=False)
    health_status_count.to_csv('./files/health_status_count.csv', index=False)

def load_final_data():
    '''
    Loads transformed and aggregated zoo data, combines them, and saves the final dataset.

    This function reads the transformed zoo data, as well as the aggregated species and health status counts.
    It then merges these datasets into a single DataFrame and saves the combined data to 'final_zoo_data.csv'.
    '''
    transformed_data = pd.read_csv('./files/transformed_zoo_data.csv')

    species_count = pd.read_csv('./files/species_count.csv')
    health_status_count = pd.read_csv('./files/health_status_count.csv')

    # Create a copy of the transformed data to avoid modifying the original DataFrame
    final_data = transformed_data.copy()

    # Add aggregated data as new columns
    final_data = final_data.assign(
        species_count=final_data['species'].map(species_count.set_index('species')['species_count']),
        health_status_count=final_data['health_status'].map(health_status_count.set_index('health_status')['count'])
    )

    # Save the final combined data to 'final_zoo_data.csv'
    final_data.to_csv('./files/final_zoo_data.csv', index=False)

def main():
    df_animals = extract_zoo_animals()
    df_health = extract_zoo_health_records()

    df_transformed = transform_data(df_animals, df_health)

    df_transformed.to_csv('./files/transformed_zoo_data.csv', index=False)

    aggregate_and_validate()
    load_final_data()

if __name__ == "__main__":
    main()