# Write function to connect MySQL
import mysql.connector

def connect_to_mysql(host, user, password, database=None):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        print("Connected to MySQL database")
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
    
# Create a new database: Since users will be uploading brand new CSV files, the system needs to create a new database for each CSV upload. Here’s how to modify the code to create a new database dynamically:
def create_new_database(connection, db_name):
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE {db_name};")
        print(f"Database {db_name} created successfully.")
    except mysql.connector.Error as err:
        print(f"Error creating database: {err}")
    finally:
        cursor.close()


# Write function to upload csv data and create table:
# Once a new database is created, we need to create a new table inside this database to store the data from the CSV file. This table should be created automatically based on the structure of the CSV file (its columns and data types). The existing function create_table_from_csv takes care of this.
import pandas as pd

def create_table_from_csv(connection, csv_file, table_name):
    df = pd.read_csv(csv_file)

    # Get column names and types
    columns = df.columns
    column_types = df.dtypes

    # Mapping pandas data types to MySQL data types
    type_mapping = {
        'object': 'VARCHAR(255)',   # Strings
        'int64': 'INT',             # Integers
        'float64': 'FLOAT',         # Decimal numbers
        'datetime64[ns]': 'DATETIME', # Date/Time values
        'bool': 'BOOLEAN'           # Boolean values
    }

    # Construct the CREATE TABLE SQL query dynamically
    create_query = f"CREATE TABLE {table_name} ("
    for col, dtype in zip(columns, column_types):
        mysql_type = type_mapping.get(str(dtype), 'VARCHAR(255)')
        create_query += f"{col} {mysql_type}, "
    
    create_query = create_query.rstrip(', ') + ');'
    
    cursor = connection.cursor()
    try:
        cursor.execute(create_query)
        print(f"Table {table_name} created successfully.")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")
    finally:
        cursor.close()

# Insert data into table
def insert_data_from_csv(connection, csv_file, table_name):
    df = pd.read_csv(csv_file)

    # Generate the INSERT INTO SQL query
    columns = df.columns
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"

    # Extract the data to insert into the table
    data = df.values.tolist()

    cursor = connection.cursor()
    try:
        cursor.executemany(insert_query, data)
        connection.commit()  # This saves the changes to the database
        print(f"Data inserted into {table_name} successfully.")
    except mysql.connector.Error as err:
        print(f"Error inserting data: {err}")
    finally:
        cursor.close()

# Main function that brings everything together
def import_csv_to_sql(host, user, password, db_name, csv_file, table_name):
    # Step 1: Connect to MySQL
    connection = connect_to_mysql(host, user, password)
    if connection is None:
        return

    # Step 2: Create a new database
    create_new_database(connection, db_name)

    # Step 3: Reconnect to the newly created database
    connection = connect_to_mysql(host, user, password, db_name)
    if connection is None:
        return

    # Step 4: Create a table based on the CSV file structure
    create_table_from_csv(connection, csv_file, table_name)

    # Step 5: Insert data from the CSV file into the table
    insert_data_from_csv(connection, csv_file, table_name)

    # Step 6: Close the MySQL connection
    connection.close()
    print(f"MySQL connection to {db_name} closed.")


######## test
host = 'localhost'
user = 'root'
password = 'Thaiduong123'
db_name = 'coffee_shop_sales'
csv_file = 'coffee_shop_sales.csv'
table_name = 'coffee_shop_sales'

import_csv_to_sql(host, user, password, db_name, csv_file, table_name)