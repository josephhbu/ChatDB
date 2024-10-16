# Query pattern building
# Class for a query pattern QueryPattern Class:
# Each pattern will represent a specific query type (e.g., "total by category").
# It includes a name (to identify the pattern), a query template (with placeholders like {category} and {measure}), and a description to explain what the pattern does.
class QueryPattern:
    def __init__(self, name, template, description):
        self.name = name           # Name of the pattern (e.g., "total_by_category")
        self.template = template   # SQL or MongoDB query template with placeholders
        self.description = description  # A brief description of what the query does

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
                # Fill in the placeholders in the template with actual values (kwargs)
                return pattern.template.format(**kwargs)
        # If the pattern is not found, raise an error
        raise ValueError(f"Pattern {pattern_name} not found")


# Step 2: Add SQL patterns (can be used for different tables and columns)

# SQL Pattern 1: Total (A) by (B) for SQL
generator.add_pattern(QueryPattern(
    "total_by_category",
    "SELECT {category}, SUM({measure}) AS total FROM {table} GROUP BY {category}",
    "Total {measure} by {category}"
))

# SQL Pattern 2: Filter and sort
generator.add_pattern(QueryPattern(
    "filter_and_sort",
    "SELECT * FROM {table} WHERE {condition} ORDER BY {sort_column} {sort_order}",
    "Filter {table} by {condition} and sort by {sort_column}"
))

# SQL Pattern 3: Count by category
generator.add_pattern(QueryPattern(
    "count_by_category",
    "SELECT {category}, COUNT(*) AS count FROM {table} GROUP BY {category}",
    "Count entries by {category}"
))

# SQL Pattern 4: Average by category
generator.add_pattern(QueryPattern(
    "average_by_category",
    "SELECT {category}, AVG({measure}) AS average FROM {table} GROUP BY {category}",
    "Average {measure} by {category}"
))

# SQL Pattern 5: Filter by date range
generator.add_pattern(QueryPattern(
    "filter_by_date_range",
    "SELECT * FROM {table} WHERE {date_column} BETWEEN '{start_date}' AND '{end_date}'",
    "Filter {table} by date range between {start_date} and {end_date}"
))

# SQL Pattern 6: Top n by measure
generator.add_pattern(QueryPattern(
    "top_n_by_measure",
    "SELECT * FROM {table} ORDER BY {measure} DESC LIMIT {n}",
    "Get top {n} results by {measure}"
))


# # MongoDB Pattern Example
# generator.add_pattern(QueryPattern(
#     "mongo_group_sum",
#     '[{{"$group": {{"_id": "${category}", "total": {{"$sum": "${measure}"}}}}}}]',
#     "Total {measure} by {category} in MongoDB"
# ))

# Example: Generate SQL query for "total_by_category"
sql_query = generator.generate_query(
    "total_by_category",
    category="product_category",   # Replace {category}
    measure="sales_amount",        # Replace {measure}
    table="sales"                  # Replace {table}
)
print("Generated SQL Query:", sql_query)

# Example: Generate SQL query for "filter_and_sort"
filter_sort_query = generator.generate_query(
    "filter_and_sort",
    table="sales",
    condition="product_category = 'Coffee'",
    sort_column="sales_amount",
    sort_order="DESC"
)
print("Generated Filter and Sort Query:", filter_sort_query)

# # Example: Generate MongoDB query for "mongo_group_sum"
# mongo_query = generator.generate_query(
#     "mongo_group_sum",
#     category="product_category",
#     measure="sales_amount"
# )
# print("Generated MongoDB Query:", mongo_query)