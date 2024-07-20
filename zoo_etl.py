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


def main():
    extract_zoo_animals()
    extract_zoo_health_records()

if __name__ == "__main__":
    main()