import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import re

# MySQL connection details
MYSQL_USER = 'root'  # Default MySQL user created by Homebrew
MYSQL_PASSWORD = ''  # No password set
MYSQL_HOST = 'localhost'  # Default host
DATABASE_NAME = 'chatDB'

# Path to the data files
FILE_PATHS = ['data/INCIDENT.csv','data/SHOOTER.csv','data/VICTIM.csv']

# Create a new MySQL database
def create_database(cursor, db):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db};")
    cursor.execute(f"USE {db};")

# Load the data file into a pandas DataFrame
def load_file(file_path):
    if file_path.endswith('.csv') or file_path.endswith('.txt'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.xlsx'):
        return pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file type.")

# Create a table in MySQL based on the DataFrame columns
def create_table_from_dataframe(df, table_name, engine):
    column_types = []
    for column in df.columns:
        # if column == 'Sources':
        #     column_types.append(f"`{column}` TEXT")
        if df[column].dtype == 'object':
            column_types.append(f"`{column}` TEXT")
        elif df[column].dtype == 'int64':
            column_types.append(f"`{column}` INT")
        elif df[column].dtype == 'float64':
            column_types.append(f"`{column}` FLOAT")
        else:
            column_types.append(f"`{column}` VARCHAR(255)") 

    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_types)});"
    with engine.connect() as connection:
        connection.execute(text(create_table_query))  # Use text() to wrap the raw SQL string

# Insert DataFrame into MySQL
def insert_dataframe_into_mysql(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

# Main implementation
def implement(file, table_name):
    # Connect to MySQL
    mydb = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )
    cursor = mydb.cursor()

    # Create a new database
    create_database(cursor, DATABASE_NAME)
    cursor.close()
    mydb.close()

    df = load_file(file)
    engine = create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{DATABASE_NAME}")

    # Create a table based on the DataFrame
    create_table_from_dataframe(df, table_name, engine)

    # Insert the data into the table
    insert_dataframe_into_mysql(df, table_name, engine)

    print(f"Data from {file} has been successfully inserted into the database {DATABASE_NAME}.")


if __name__ == "__main__":
    for file in FILE_PATHS:
        table_name = ''
        match = re.search(r'/([A-Z]+)\.csv$', file)
        if match:
            table_name = match.group(1).lower()
        implement(file, table_name)

    print(f"All files have been successfully inserted into the database {DATABASE_NAME}.")


