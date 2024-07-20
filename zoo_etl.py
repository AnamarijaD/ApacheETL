import pandas as pd

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

def main():
    df_animals = extract_zoo_animals()
    df_health = extract_zoo_health_records()

    df_transformed = transform_data(df_animals, df_health)

    df_transformed.to_csv('./files/transformed_zoo_data.csv', index=False)

if __name__ == "__main__":
    main()