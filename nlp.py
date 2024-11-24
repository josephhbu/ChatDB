import re
from query_patterns import generator
# Assume we have an existing 'extract_params' function to extract parameters

# MongoDB Query Execution
def run_mongo_query(query, db_name):
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    result = db.command(query)
    return result

# Function to run a SQL query
def run_sql_query(query, engine):
    # with engine.connect() as connection:
    #     result = connection.execute(text(query))
    #     return result.fetchall()
    cursor = engine.cursor()
    res = []
    try:
        cursor.execute(query)
    
        res = cursor.fetchall()
    except:
        print('Run query error!')
    finally:
        cursor.close()
        return res
        # engine.close()
    

# Steps:
# 1. Extract Parameters Dynamically: Use regex to extract parameters from the natural language input.
# 2. Map Intent to the Query Pattern: Use the detected intent to choose the appropriate query template.
# 3. Generate Queries Dynamically: Once parameters are extracted, use them to generate the query using the pattern's template depending on the database chosen (MongoDB or SQL).

# Function to detect the intent of the user input


def process_user_input_sql(user_input, intent, engine):

    result = []

    if intent == "join_query": #most complicated
        # Updated pattern for join queries
        pattern = r"show\s+(?P<table1>\w+)\s+which\s+has\s+(?P<table2>\w+)\s+that\s+the\s+(?P<column>\w+)\s+(is|=)\s+(?P<value>.+)"
        params = extract_params(user_input, pattern)
        if not params or not all(k in params for k in ['table1', 'table2', 'column', 'value']):
            raise ValueError("Invalid or missing parameters for join query.")

        # Fetch column information for both tables
        cursor = engine.cursor()
        cursor.execute(f"DESC {params['table1']}")
        columnsOf1 = [column[0] for column in cursor.fetchall()]

        cursor.execute(f"DESC {params['table2']}")
        columnsOf2 = [column[0] for column in cursor.fetchall()]
        cursor.close()
        # Identify joinable columns
        join_column1 = next((col for col in columnsOf1 if 'id' in col.lower()), None)
        join_column2 = next((col for col in columnsOf2 if 'id' in col.lower()), None)

        if not join_column1 or not join_column2:
            raise ValueError(f"No joinable columns found between {params['table1']} and {params['table2']}.")
        # Generate the query
        query = f"""
        SELECT {params['table1']}.* 
        FROM {params['table1']} 
        INNER JOIN {params['table2']} 
        ON {params['table1']}.{join_column1} = {params['table2']}.{join_column2}
        WHERE {params['table2']}.{params['column']} = '{params['value']}'
        """.strip()
        print("Generated Join Query:", query)
        # Execute the query
        result = run_sql_query(query, engine)
    
    elif intent == "filter_by_date_range":
        pattern = r"show\s+(?P<table>\w+)\s+where\s+(?P<date_column>\w+)\s+is\s+between\s+'(?P<start_date>[\d-]+)'\s+and\s+'(?P<end_date>[\d-]+)'"
        params = extract_params(user_input, pattern)
        
        if params:
            # Use generator to construct the query
            query = generator.generate_query(
                "filter_by_date_range",
                table=params["table"],
                date_column=params["date_column"],
                start_date=params["start_date"],
                end_date=params["end_date"]
            )
            print(f"Generated Query: {query}")
            result = run_sql_query(query, engine)
            print("SQL Query Result:", result)

    elif intent == 'basic_select':
        # Flexible pattern for basic select queries
        if " of " in user_input:
            pattern = r"(get|show)\s+(?P<columns>[\w\s,]+)\s+of\s+(?P<table>\w+)\s*(?:where\s+(?P<column>\w+)\s+(is|=)\s+(?P<value>[\w\s]+))?"
        else:
            user_input = user_input.replace(" all ", " ").replace(" every ", " ")    
            pattern = r"(get|show)\s+(?P<table>\w+)\s*(?:where\s+(?P<column>\w+)\s+(is|=)\s+(?P<value>\w+))?"
        user_input = user_input.replace(" me ", " ")
        params = extract_params(user_input, pattern)

        if params:
            # Dynamically construct the condition
            condition = None
            if params.get("column") and params.get("value"):
                condition = f"{params['column']} = '{params['value']}'"


            columns = "*"
            
            if 'columns' in params:
                columns = params['columns']
            # Generate the SQL query
            query = generator.generate_query(
                "basic_select",
                table=params["table"],
                condition=condition if condition else "1=1",  # Default to no filtering
                columns=columns
            )
            print("Generated Basic Select Query:", query)

            # Run the query
            result = run_sql_query(query, engine)
    elif intent == "total_group_by":
        pattern = r"total (?P<measure>[\w\s]+) by (?P<category>\w+) from (?P<table>\w+)"
        params = extract_params(user_input, pattern)
        if params:
            query = generator.generate_query(
                "total_by_category",
                category=params["category"],
                measure=params["measure"].strip().replace(" ", "_"),  # Convert multi-word measures to column format if needed
                table=params["table"]
            )
            result = run_sql_query(query, engine)  # Run SQL query
            print("Generated and Executed SQL Group By Query:", query)
            print("SQL Query Result:", result)
    
    elif intent == "average_by_category":
        pattern = r"average (?P<measure>[\w\s]+) by (?P<category>\w+) from (?P<table>\w+)"
        params = extract_params(user_input, pattern)
        if params:
            query = generator.generate_query(
                "average_by_category",
                category=params["category"],
                measure=params["measure"].strip().replace(" ", "_"),  # Convert multi-word measures to column format if needed
                table=params["table"]
            )
            result = run_sql_query(query, engine)  # Run SQL query
            print("Generated and Executed SQL Group By Query:", query)
            print("SQL Query Result:", result)
             
    elif intent == "filter_sort":
        pattern = r"find\s+(?P<columns>(?:\w+\s*,\s*)*\w+)\s+from\s+(?P<table>\w+)\s+where\s+(?P<condition>.+)\s+order\s+by\s+(?P<sort_column>\w+)\s+(?P<sort_order>asc|desc)"
        params = extract_params(user_input, pattern)
        if params:
            # Split and clean up the column list
            columns = ', '.join([col.strip() for col in params['columns'].split(',')])

            # Fix condition syntax (replace "is" with "=" for SQL compliance)
            
            condition  = params["condition"].replace(params["condition"].split("is ")[-1],f'\'{params["condition"].split("is ")[-1]}\'')
            condition = condition.replace(" is ", " = ")

            # Generate the query dynamically using the generator
            query = generator.generate_query(
                "filter_and_sort",
                columns=columns,  # Dynamically include columns
                table=params["table"],
                condition=condition,
                sort_column=params["sort_column"],
                sort_order=params["sort_order"].upper()  # Convert sort order to uppercase for SQL compliance
            )
            print(f"Generated Filter and Sort Query: {query}")

            # Execute the query
            result = run_sql_query(query, engine)
            print("SQL Query Result:", result)

    elif intent == "count_by_category":
        pattern = r"count (?P<table>\w+) by (?P<category>\w+)"
        params = extract_params(user_input, pattern)
        if params:
            query = generator.generate_query(
                "count_by_category",
                category=params["category"],
                table=params["table"]
            )
            result = run_sql_query(query, engine)  # Run SQL query
            print("Generated and Executed SQL Count By Query:", query)
            print("SQL Query Result:", result)

    elif intent == "list_tables":
        query = generator.generate_query("list_tables")
        result = run_sql_query(query, engine)
        print("Generated List Tables Query:", query)
        print("SQL Query Result:", result)
    
    
    elif intent == 'top_n_by_measures':
        query = generator.generate_query("list_tables")
        tables = run_sql_query(query, engine)
        tables =  [item[0].lower() for item in tables]
        
        trim_input = user_input.replace("get me ", "").replace('get ', '')
        tokens = trim_input.split()
        number = tokens[0]
        extremes = ['highest', 'lowest', 'largest', 'smallest']
        mid = 'mid'
        for extreme in extremes:
            if extreme in trim_input:
                mid = extreme

        if mid in ['highest', 'largest']:
            sort = 'DESC'
        else:
            sort = 'ASC'

        if tokens[1] in tables:
            table = tokens[1]
            measure = tokens[-1]
            print( number, table, measure, sort) 
            print('NORMAL GET ORDER BY MEASURE')
            query = generator.generate_query(
                "top_n_by_measure_table",
                measure=measure,
                table=table,
                n=number,
                sort=sort
            )
            result = run_sql_query(query, engine)
        else:
           
            later_half = trim_input.split(mid)[-1]

            chosen_table = ''
            for table in tables:
                if table in later_half:
                    chosen_table = table
            measure =tokens[1]
            if chosen_table != '':
                print( number, chosen_table, measure, sort) 
                print('COUNT MEASURE')
                query = generator.generate_query(
                    "top_n_by_measure_count",
                    measure=measure,
                    table=chosen_table,
                    n=number,
                    sort=sort
                )
                result = run_sql_query(query, engine)
            else:
                column = tokens[1].strip()
                measure = later_half.strip().replace("number of ", "")
                
                for table in tables:
                    query = 'SHOW COLUMNS FROM ' + table.upper()
                    cols = run_sql_query(query, engine)
                    cols = [item[0].lower() for item in cols]
                    if column in cols  and measure in cols:
                        chosen_table = table                            
                        break
                
                print(number, column, measure, sort, chosen_table) #
                print('select sum')
                query = generator.generate_query(
                    "top_n_by_measure_no_table",
                    measure=measure,
                    table=chosen_table,
                    n=number,
                    sort=sort,
                    column=column
                )
                result = run_sql_query(query, engine)
        print("Generated List Tables Query:", query)
        print("SQL Query Result:", result)
    elif intent == "describe_attr":
        # Extract the table name from the user input
        pattern = r"show\s+table\s+(?P<table>\w+)\s+attributes"
        params = extract_params(user_input, pattern)

        if not params or 'table' not in params:
            raise ValueError("Invalid or missing table name for describe attributes intent.")

        table_name = params["table"]

        # Generate and execute the DESCRIBE query
        query = f"SHOW COLUMNS FROM {table_name}"
        print(f"Generated Describe Query: {query}")
        result = run_sql_query(query, engine)
        print("SQL Query Result:", result)
    return result

def process_user_input_mongodb(user_input, intent, engine):
    result = []
    if intent == "join_query":
        # Extract parameters for a join query
        pattern = "join_query {columns} from {table1} join {table2} on {join_column} where {condition}"
        params = extract_params(user_input, pattern)

        if params:
            if db_type == 'MongoDB':
                query = generator.generate_query(
                    "mongo_lookup",
                    table1=params["table1"],
                    table2=params["table2"],
                    local_field=params["join_column"],
                    foreign_field=params["join_column"],  # Same field in this example
                    join_as=params["columns"],
                    condition=params["condition"]
                )
                print("Generated Join Query (MongoDB):", query)
            else:
                print(f"Unable to extract parameters from: {user_input}")

    elif intent == "total_group_by":
        # Extract parameters for a 'total_by_category' query
        pattern = "total {measure} by {category} from {table}"
        params = extract_params(user_input, pattern)

        if params:
            if db_type == 'MongoDB':
                query = generator.generate_query(
                    "mongo_group_sum",
                    category=params["category"],
                    measure=params["measure"]
                )
                result = run_mongo_query(query)  # Run MongoDB query
                print("Generated and Executed MongoDB Group By Query:", query)
                print("MongoDB Query Result:", result)

    elif intent == "filter_sort":
        # Extract parameters for a filter and sort query
        pattern = "find {columns} from {table} where {condition} order by {sort_column} {sort_order}"
        params = extract_params(user_input, pattern)

        if params:
            if db_type == 'MongoDB':
                query = generator.generate_query(
                    "mongo_filter_and_sort",
                    condition=params["condition"],
                    sort_column=params["sort_column"],
                    sort_order=params["sort_order"]
                )
                result = run_mongo_query(query)  # Run MongoDB query
                print("Generated and Executed MongoDB Filter Sort By Query:", query)
                print("MongoDB Query Result:", result)

    elif intent == "count_by_category":
        # Extract parameters for count by category query
        pattern = "count {measure} by {category} from {table}"
        params = extract_params(user_input, pattern)

        if params:
            if db_type == 'MongoDB':
                query = generator.generate_query(
                    "mongo_count_by_category",
                    category=params["category"]
                )
                result = run_mongo_query(query)  # Run MongoDB query
                print("Generated and Executed MongoDB Count By Query:", query)
                print("MongoDB Query Result:", result)

    elif intent == "list_collections":
        # Generate query for listing collections in MongoDB
        if db_type == 'MongoDB':
            query = generator.generate_query("mongo_list_collections")
            result = run_mongo_query(query)  # Run MongoDB query
            print("Generated List Collections Query:", query)
            print("MongoDB Query Result:", result)

def detect_intent(user_input):
    user_input = user_input.lower().strip()

    # Define intent patterns with priorities
    intent_patterns = [
        ("join_query", r"\b(show|get)\b\s+(?P<table1>\w+)\s+\bwhich has\b\s+(?P<table2>\w+)\s+\bthat the\b\s+(?P<column>\w+)\s+(is|=)\s+(?P<value>\w+)"),  # Specific pattern for join queries
        ("total_group_by", r"\btotal\b.*\bby\b.*"),  # Pattern for group by queries
        ("filter_sort", r"\bfind\b.*\bwhere\b.*\border by\b.*"),  # Pattern for filter and sort queries
        ("count_by_category", r"\bcount\b.*\bby\b.*"),  # Pattern for count by category
        ("average_by_category", r"\baverage\b|\bmean\b.*\bof\b.*"),  # Pattern for average by category
        ("filter_by_date_range", r"\bshow\b.*\bfrom\b.*\bto\b.*"),  # Pattern for date range filters
        ("basic_select", r"\b(get|show)\b.*\bwhere\b"),  # Pattern for basic select queries
        ("list_tables", r"\bshow\b.*\btables\b"),  # Pattern for listing tables
        ("list_collections", r"\blist\b.*\bcollections\b"),  # Pattern for listing collections
        # ("top_n_by_measure", r"\btop\b.*\bwhere\b.*"),  # Pattern for top N queries
        ("describe_attr", r"\btable\b.*\battributes\b")
    ]

    # Check patterns in priority order
    for intent, pattern in intent_patterns:
        if re.search(pattern, user_input):
            return intent

    extremes = ['highest', 'lowest', 'largest', 'smallest']
    for extreme in extremes:
        if extreme in user_input:
            return 'top_n_by_measures'


    # Default intent if no patterns match
    return "unknown"





# Extract parameters dynamically from the natural language query
def extract_params(nl_query, pattern):
    """
    Extract parameters dynamically from a natural language query based on the provided pattern.
    
    Args:
        nl_query (str): The natural language query input from the user.
        pattern (str): The regex pattern to match and extract parameters.
    
    Returns:
        dict: A dictionary of extracted parameters or None if no match is found.
    """
    try:
        # Convert the natural language pattern into a regex pattern
        regex = re.compile(pattern, re.IGNORECASE)  # Case-insensitive matching
        print(f"Using Regex Pattern: {regex}")
        # Attempt to match the regex pattern to the user query
        match = regex.match(nl_query.strip())
        
        if not match:
            print(f"Could not extract parameters from: '{nl_query}'")
            print(f"Tokenized input for debugging: {nl_query.split()}")
            return None

        # Extract parameters into a dictionary
        data_dict = match.groupdict()
        print(f"Extracted Parameters: {data_dict}")

        # Adjust special cases for 'columns'
        if 'columns' in data_dict and data_dict['columns'].strip().lower() in ['all', '*']:
            data_dict['columns'] = '*'
        
        if data_dict['table'][-1] == 's':
            data_dict['table'] = data_dict['table'][0:-1]
        
        return data_dict

    except re.error as re_err:
        print(f"Regex error: {re_err}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred in extract_params: {e}")
        return None


# Main function for processing the user input
def process_user_input(user_input, db_type, engine):
    # Step 1: Detect intent
    intent = detect_intent(user_input)
    print(intent)
    if intent == 'unknown':
        print("Cannot detect intention")
        return []

    data_from_db =  []
    if db_type == 'SQL':
        data_from_db = process_user_input_sql(user_input, intent, engine)
    elif db_type == 'MongoDB':
        data_from_db = process_user_input_mongodb(user_input, intent, engine)
    else:
        print("Invalid DB Type")
        return []
    
    return data_from_db


