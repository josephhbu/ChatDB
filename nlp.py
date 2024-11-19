import re

def detect_intent(user_input):
    # Example patterns for total and filter queries
    if re.search(r'total.*by.*', user_input.lower()):
        return "group_by"
    elif re.search(r'find.*where.*', user_input.lower()):
        return "filter_sort"
    elif re.search(r'count.*by.*', user_input.lower()):
        return "count_by_category"
    elif re.search(r'average.*of.*', user_input.lower()):
        return "average_by_category"
    elif re.search(r'show.*from.*to.*', user_input.lower()):
        return "filter_by_date_range"
    elif re.search(r'top.*where.*', user_input.lower()):
        return "top_n_by_measure"
    else:
        return "unknown"

# Example usage:
user_input = "Show the total sales by product type"
intent = detect_intent(user_input)
print(intent)  # Output: "group_by"

### This is a test for local git