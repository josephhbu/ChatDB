sql_query_patterns = [
    {
        "name": "total_by_category",
        "sql": "SELECT {category}, SUM({measure}) AS total FROM {table} GROUP BY {category}",
        "description": "Total {measure} by {category}"
    },
    {
        "name": "filter_sort",
        "sql": "SELECT {columns} FROM {table} WHERE {condition} ORDER BY {sort_column} {sort_order}",
        "description": "find {columns} from {table} where {condition} order by {sort_column} {sort_order}"
    },
    {
        "name": "count_by_category",
        "sql": "SELECT {category}, COUNT(*) AS count FROM {table} GROUP BY {category}",
        "description": "Count entries by {category}"
    },
    {
        "name": "average_by_category",
        "sql": "SELECT {category}, AVG({measure}) AS average FROM {table} GROUP BY {category}",
        "description": "Average {measure} by {category}"
    },
    {
        "name": "filter_by_date_range",
        "sql": "SELECT * FROM {table} WHERE CONVERT(DATE, {date_column}) >= '{start_date}' AND CONVERT(DATE, {date_column}) <= '{end_date}' ",
        "description": "Filter {table} by date range between {start_date} and {end_date}"
    },
    {
        "name": "top_n_by_measure_table",
        "sql": "SELECT * FROM {table} ORDER BY {measure} {sort_order} LIMIT {n}",
        "description": "get me {n} {table} with highest {measure}"
    },
    {
        "name": "top_n_by_measure_no_table",
        "sql": "SELECT {column}, SUM({measure}) as total_{measure} FROM {table} GROUP BY {column} ORDER BY total_{measure} {sort_order} LIMIT {n}",
        "description": "get me {n} {column} with highest number of {measure}"
    },
    {
        "name": "top_n_by_measure_count",
        "sql": "SELECT {measure}, COUNT(*) as counting FROM {table} GROUP BY {measure} ORDER BY counting {sort_order} LIMIT {n}",
        "description": "get me {n} {table} with highest {measure}"
    },
    # complicate layer, Manually format the SQL query to ensure clarity in nlp.py
    # {
    #     "name": "join_query",
    #     "sql": "SELECT {table1}.* FROM {table1} INNER JOIN {table2} ON {table1}.{join_column1} = {table2}.{join_column2} WHERE {table2}.{columns} = '{condition}'",
    #     "description": "Join {table1} with {table2} on {join_column} with condition {condition}"
    # },
    {
        "name": "basic_select",
        "sql": "SELECT {columns} FROM {table} WHERE {condition}",
        "description": "get me {columns} of {table} where {condition}"
    },
    {
        "name": "list_tables",
        "sql": "SHOW TABLES",
        "description": "show tables"
    }
]

# MongoDB Patterns
# Define all MongoDB query patterns
mongo_query_patterns = [
    {
        "name": "mongo_group_sum",
        "query": '[{{"$group": {{"_id": "${field}", "total": {{"$sum": "${numeric_field}"}}}}}}]',
        "description": "Calculate the total of {numeric_field} grouped by {field} in {collection}"
    },
    {
        "name": "mongo_filter_and_sort",
        "query": '[{{"$match": {{"{field}": "{value}"}}}}, {{"$sort": {{"{field}": {{"$direction": "{sort_order}"}}}}}}]',
        "description": "Filter and sort MongoDB collection by {field} and sort in {sort_order} order"
    },
    {
        "name": "mongo_count_by_category",
        "query": '[{{"$group": {{"_id": "${field}", "count": {{"$sum": 1}}}}}}]',
        "description": "Count documents grouped by {field} in {collection}"
    },
    {
        "name": "mongo_average_by_category",
        "query": '[{{"$group": {{"_id": "${field}", "average": {{"$avg": "${numeric_field}"}}}}}}]',
        "description": "Calculate the average of {numeric_field} grouped by {field} in {collection}"
    },
    {
        "name": "mongo_filter_by_date_range",
        "query": '[{{"$match": {{"{date_field}": {{"$gte": "{start_date}", "$lte": "{end_date}"}}}}}}]',
        "description": "Filter documents in {collection} where {date_field} is between {start_date} and {end_date}"
    },
    {
        "name": "mongo_top_n_by_measure",
        "query": '[{{"$sort": {{"{numeric_field}": -1}}}}, {{"$limit": {n}}}]',
        "description": "Retrieve top {n} documents in {collection} sorted by {numeric_field}"
    },
    {
        "name": "mongo_lookup",
        "query": '[{{"$lookup": {{"from": "{table}", "localField": "{field}", "foreignField": "{field}", "as": "{join_as}"}}}}]',
        "description": "Join {collection} with {table} on {field} in MongoDB"
    },
    {
        "name": "mongo_basic_select",
        "query": '[{{"$match": {{"{field}": "{value}"}}}}]',
        "description": "Select documents from {collection} where {field} is {value}"
    },
    {
        "name": "mongo_insert_query",
        "query": '[{{"insert": "{collection}", "documents": [{value}]}}]',
        "description": "Insert documents into {collection} in MongoDB"
    },
    {
        "name": "mongo_update_query",
        "query": '[{{"update": "{collection}", "updates": [{{"q": {{"{field}": "{value}"}}, "u": {{"$set": {columns}}}}]}}]',
        "description": "Update documents in {collection} where {field} is {value}"
    },
    {
        "name": "mongo_delete_query",
        "query": '[{{"delete": "{collection}", "deletes": [{{"q": {{"{field}": "{value}"}}, "limit": 1}}]}}]',
        "description": "Delete documents from {collection} where {field} is {value}"
    },
    {
        "name": "mongo_list_collections",
        "query": 'db.getCollectionNames()',
        "description": "List all collections in MongoDB"
    }
]


# Query pattern building
# Class for a query pattern QueryPattern Class:
# Each pattern will represent a specific query type (e.g., "total by category").
# It includes a name (to identify the pattern), a query template (with placeholders like {category} and {measure}), and a description to explain what the pattern does.
class QueryPattern:
    def __init__(self, name, template, description):
        self.name = name           # Name of the pattern (e.g., "total_by_category")
        self.template = template   # SQL or MongoDB query template with placeholders
        self.description = description  # A brief description of what the query does
        # self.conditionclase = 


# Class to manage multiple patterns and generate queries
# This class manages a collection of query patterns.
# You can add new query patterns to it.
# You can generate SQL or MongoDB queries by selecting the pattern and filling in the required placeholders (like the table name, column names, etc.).
class QueryGenerator:
    def __init__(self):
        self.patterns = []  # List to store all query patterns

    # Function to add new patterns to the generator
    def add_pattern(self, pattern):
        self.patterns.append(pattern)

    # Function to generate a query based on the pattern name
    def generate_query(self, pattern_name, **kwargs):
        for pattern in self.patterns:
            if pattern.name == pattern_name:
                print(pattern.template.format(**kwargs))

                # Fill in the placeholders in the template with actual values (kwargs)
                try:
                    return pattern.template.format(**kwargs)
                except KeyError as e:
                    raise ValueError(f"Missing parameter: {e}")
        # If the pattern is not found, raise an error
        raise ValueError(f"Pattern {pattern_name} not found")

# Initialize the query generator
generator = QueryGenerator()

# Add MongoDB patterns to the generator
for pattern in mongo_query_patterns:
    generator.add_pattern(QueryPattern(
        pattern["name"],
        pattern["query"],
        pattern["description"]
    ))


for pattern in sql_query_patterns:
     generator.add_pattern(QueryPattern(
        pattern["name"],
        pattern["sql"],
        pattern["description"]
    ))