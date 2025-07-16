import sqlite3

def view_table_data(table_name):
    # Connect to SQLite database
    conn = sqlite3.connect('pricing_data.db')
    cursor = conn.cursor()

    # Query to fetch data from the specified table
    cursor.execute(f"SELECT * FROM {table_name};")
    rows = cursor.fetchall()

    print(f"Data in the '{table_name}' table:")
    for row in rows:
        print(row)

    # Close the connection
    conn.close()

# Call the function to view data in the 'products' table
view_table_data('products')
