import requests
import pandas as pd
import sqlite3


# Step 1: Extract Data from API
def extract_data():
    url = "https://jsonplaceholder.typicode.com/posts"  # Example API (replace with your API URL)
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()  # Extracts JSON data from the response
        print(f"Data Extracted: {data[:5]}")  # Print first 5 records for preview
        return data
    else:
        print("Failed to retrieve data from the API")
        return None


# Step 2: Transform Data
def transform_data(data):
    # Convert the data to a DataFrame for easier manipulation
    df = pd.DataFrame(data)

    # Selecting only relevant columns (id, title, and body in this case)
    df = df[['id', 'title', 'body']]

    print(f"Transformed Data:\n{df.head()}")  # Preview the transformed data
    return df


# Step 3: Load Data into SQLite
def load_data(df):
    # Connect to SQLite database (it will create a new one if not exists)
    conn = sqlite3.connect('etl_data.db')  # Database name is 'etl_data.db'
    cursor = conn.cursor()

    # Clear the table before inserting new data (to avoid UNIQUE constraint violations)
    cursor.execute("DELETE FROM posts")

    # Insert transformed data into the table
    df.to_sql('posts', conn, if_exists='append', index=False)

    # Commit and close the connection
    conn.commit()
    conn.close()
    print("Data successfully loaded into SQLite.")


# Main function to run the ETL pipeline
if __name__ == "__main__":
    # Step 1: Extract
    data = extract_data()

    if data:
        # Step 2: Transform
        transformed_data = transform_data(data)

        # Step 3: Load
        load_data(transformed_data)