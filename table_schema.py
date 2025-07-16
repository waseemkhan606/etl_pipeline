import sqlite3

def check_table_schema(table_name):
    # Connect to SQLite database
    conn = sqlite3.connect('pricing_data.db')
    cursor = conn.cursor()

    # Query to get table schema
    cursor.execute(f"PRAGMA table_info({table_name});")
    schema = cursor.fetchall()

    print(f"Schema for '{table_name}' table:")
    for column in schema:
        print(column)

    # Close the connection
    conn.close()

# Call the function to check the schema of the 'products' table
check_table_schema('products')
