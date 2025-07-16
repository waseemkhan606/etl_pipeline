import sqlite3

def list_tables():
    # Connect to SQLite database
    conn = sqlite3.connect('pricing_data.db')
    cursor = conn.cursor()

    # Query to list all tables in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    print("Tables in the database:")
    for table in tables:
        print(table[0])  # Printing table name

    # Close the connection
    conn.close()

# Call the function to list the tables
list_tables()

