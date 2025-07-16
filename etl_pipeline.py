import requests
import pandas as pd
import sqlite3


# Step 1: Extract Pricing Data from Public API
def extract_data():
    url = "https://fakestoreapi.com/products"  # Public API providing product pricing data
    response = requests.get(url)

    # Print the status code and headers to help debug
    print(f"Response Status Code: {response.status_code}")  # Print status code
    print(f"Response Headers: {response.headers}")  # Print response headers
    print(f"Response Content: {response.text}")  # Print the raw content of the response

    # Check if the response is valid
    if response.status_code == 200:
        try:
            data = response.json()  # Convert the response to JSON format
            print(f"Data Extracted: {data[:5]}")  # Print the first 5 records for preview
            return data
        except ValueError:
            print("Error: Unable to decode the response as JSON.")
            return None
    else:
        print(f"Failed to retrieve data from the API. Status Code: {response.status_code}")
        return None


# Step 2: Transform Data for Pricing Optimization
def transform_data(data):
    df = pd.DataFrame(data)

    # Print raw data for debugging
    print(f"Raw Data: {data}")

    # Transformations
    df['price'] = df['price'].round(2)  # Round the prices to two decimal places
    baseline_price = 20.0
    df['price_difference'] = df['price'] - baseline_price

    # Keep relevant columns
    df = df[['id', 'title', 'price', 'category', 'price_difference']]

    # Print transformed data for debugging
    print(f"Transformed Data:\n{df.head()}")  # Print the first 5 rows of the transformed data

    return df


# Step 3: Load Data into SQLite Database
def load_data(df):
    # Connect to SQLite database
    conn = sqlite3.connect('pricing_data.db')  # Database name is 'pricing_data.db'
    cursor = conn.cursor()

    # Clear the table before inserting new data (optional, use with caution)
    cursor.execute("DELETE FROM products")

    # Insert the transformed data into the table
    df.to_sql('products', conn, if_exists='append', index=False)

    # Commit and close the connection
    conn.commit()
    conn.close()

    print(f"Loaded {len(df)} records into the database.")


# Check the number of records in the database
def count_records():
    # Connect to SQLite database
    conn = sqlite3.connect('pricing_data.db')
    cursor = conn.cursor()

    # Query to count records
    cursor.execute("SELECT COUNT(*) FROM products")
    count = cursor.fetchone()[0]
    conn.close()

    print(f"Number of records in the database: {count}")


# Main function to run the ETL pipeline
if __name__ == "__main__":
    # Step 1: Extract
    data = extract_data()

    if data:
        # Step 2: Transform
        transformed_data = transform_data(data)

        # Step 3: Load
        load_data(transformed_data)

        # Check if the data is loaded into the database
        count_records()
