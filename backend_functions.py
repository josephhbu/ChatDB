import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

MYSQL_USER = 'user'
MYSQL_PASSWORD = 'Dsci-551'
MYSQL_HOST = 'localhost'
DATABASE_NAME = 'coffee_shop_sales'

#FILE_PATH = 'your_file.csv'

#Create new mysql db
def create_database(cursor, db):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db};")
    cursor.execute(f"USE {db};")

#Load file into df
def load_file(file_path):
    if file_path.endswith('.csv') or file_path.endswith('.txt'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.xlsx'):
        return pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file type.")

#Create table in mysql based on df columns
def create_table_from_dataframe(df, table_name, engine):
    column_types = []
    for column in df.columns:
        if df[column].dtype == 'object':
            column_types.append(f"`{column}` VARCHAR(50)")
        elif df[column].dtype == 'int64':
            column_types.append(f"`{column}` INT")
        elif df[column].dtype == 'float64':
            column_types.append(f"`{column}` FLOAT")
        else:
            column_types.append(f"`{column}` VARCHAR(50)")

    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_types)});"
    with engine.connect() as connection:
        connection.execute(create_table_query)

#Insert df into mysql
def insert_dataframe_into_mysql(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

# Main function - chatgpt did this part for me
def implement():
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

    # Load the data file into a pandas DataFrame
    df = load_file(FILE_PATH)

    # Create a MySQL engine
    engine = create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{DATABASE_NAME}")

    # Create a table based on the DataFrame
    table_name = "uploaded_data"
    create_table_from_dataframe(df, table_name, engine)

    # Insert the data into the table
    insert_dataframe_into_mysql(df, table_name, engine)

    print(f"Data from {FILE_PATH} has been successfully inserted into the database {DATABASE_NAME}.")

if __name__ == "__main__":
    implement()
