
import streamlit as st
from sqlalchemy import create_engine
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import random
import os

from nlp_logic.nlp import process_user_input
from nlp_logic.query_patterns import generator
from nlp_logic.query_suggestions import process_sample_queries
from backend.backend_functions import implement
from backend.nosql_backend import import_multiple_json_to_mongodb, csv_to_json
from nlp_logic.mongo_queries import process_user_input_mongodb
from nlp_logic.mongo_NLP import parse_query, execute_query

sql_examples = []
mongodb_examples = []

def display_example_queries(db_type):
    if db_type == "SQL":
        example_queries = random.sample(sql_examples, 3)
        st.write("Here are 3 example SQL queries:")
    elif db_type == "MongoDB":
        example_queries = random.sample(mongodb_examples, 3)
        st.write("Here are 3 example MongoDB queries:")

    for i, query in enumerate(example_queries, 1):
        st.markdown(f"**{i}.**")
        st.code(query, language="sql" if db_type == "SQL" else "json") 

# Save uploaded file to a temporary directory
def save_uploaded_file(uploaded_file):
    file_path = os.path.join("uploads", uploaded_file.name)
    os.makedirs("uploads", exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path

def get_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",  # Update with your MySQL username
            password="",  # Update with your MySQL password
            database="chatDB"  # Replace with your database name
        )
        if connection.is_connected():
            return connection
    except Exception as e:
        st.error(f"Error connecting to MySQL: {e}")
        return None
    
def get_sql_engine():
    return create_engine('mysql+mysqlconnector://root@localhost/db')

def get_mongo_client():
    return MongoClient('mongodb://localhost:27017/')

# Main function for the Streamlit app
def main():
    st.set_page_config(layout="wide")
    st.title("ChatDB: Interactive Database Query Assistant")

    # Sidebar for database selection
    db_type = st.sidebar.selectbox("Select Database Type", ["SQL", "MongoDB"])
    
    # Initialize database connections
    mysql_connection = get_mysql_connection() if db_type == "SQL" else None
    mongo_client = get_mongo_client() if db_type == "MongoDB" else None

    st.write(f"Using {db_type} database")

    # Section for uploading datasets
    st.sidebar.subheader("Upload Dataset")
    uploaded_file = st.sidebar.file_uploader("Choose a CSV file", type=["csv"])
    table_name = st.sidebar.text_input("Enter Table/Collection Name")
    if uploaded_file and table_name:
        try:
            file_path = save_uploaded_file(uploaded_file)
            if db_type == "SQL":
                st.subheader("SQL Database Operations")
                if file_path.lower().endswith('.csv'):
                    os.environ["IMPLEMENT_FILE_PATH"] = file_path  
                    implement(file_path, table_name)  # Run the existing SQL data upload function
                    st.success(f"Dataset '{uploaded_file.name}' uploaded to SQL as table '{table_name}' successfully.")
                else:
                    st.error("Unsupported file format. Please upload a CSV.")
            elif db_type == "MongoDB":
                st.subheader("MongoDB Operations")
                if file_path.lower().endswith('.csv'):
                    json_file_path = f"{table_name}.json"
                    json_data = csv_to_json(file_path, json_file_path)
                    if json_data is not None:
                        import_multiple_json_to_mongodb([json_file_path], "chatDB")
                        st.success(f"CSV file '{uploaded_file.name}' converted and uploaded to MongoDB as collection '{table_name}' successfully.")
                    else:
                        st.error("Failed to convert CSV to JSON")
                else:
                    st.error("Unsupported file format. Please upload a CSV or JSON file.")

        except Exception as e:
            st.error(f"Error uploading dataset: {e}")

    # Text input for user query
    user_input = st.text_input("Enter your query (natural language or pattern-based):")
        
    # Example queries section
    if "example" in user_input.lower():
        if db_type == "SQL":
            st.write("Here are some examples of SQL queries you can try:")
            result = process_sample_queries(user_input, db_type, engine=mysql_connection)
        else:
            st.write("Here are some examples of MongoDB queries you can try:")
            result = process_sample_queries(user_input, db_type, db_name='chatDB')
            st.write(result)
            
        for i, query in enumerate(result, 1):
            st.markdown(f"**{i}.** {query.get('description')}")
            st.code(query.get('query'), language="sql" if db_type == "SQL" else "json") 

    elif user_input:
        try:
            if db_type == "SQL":
                # Run SQL query and display results
                sql_query, result = process_user_input(
                    user_input=user_input,
                    db_type=db_type,
                    engine=mysql_connection if db_type == "SQL" else mongo_client
                )
                if sql_query:
                    st.write("Generated SQL Query:")
                    st.code(sql_query, language="sql")
                if result:
                    res, headers = result
                    st.write("Query Results:")
                    df = pd.DataFrame(res, columns=headers)
                    st.dataframe(df, use_container_width=True) 
                else:
                    st.warning("No results found or query failed.")
            elif db_type == "MongoDB":
                # Process MongoDB query and display results
                user_query = parse_query(user_input)
                result = execute_query(user_query)
                if result:  
                    mongo_query = result["query"]
                    res = result["result"]
                    st.write("Generated MongoDB Query:")
                    st.code(mongo_query, language="json")
                    st.write("Query Results:")
                    if isinstance(res, list):
                        st.json(res)  # Display as JSON
                    elif isinstance(res, dict):
                        st.json(res)  # Display a single document
                    else:
                        st.write(res)
                else:
                    st.warning("No results found or query failed.")
        except Exception as e:
            st.error(f"An error occurred: {e}")

    # Close MySQL connection when the app ends
    if mysql_connection:
        mysql_connection.close()
       

if __name__ == "__main__":
    main()
