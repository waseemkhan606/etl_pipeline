import pytest
import requests
import pandas as pd
from etl_pipeline import extract_data, transform_data, load_data
import sqlite3


# Test the extraction of pricing data
def test_extract_data():
    data = extract_data()
    assert data is not None, "Extracted data should not be None"
    assert len(data) > 0, "Extracted data should have more than 0 records"
    assert 'price' in data[0], "'price' field should exist in the data"
    assert 'title' in data[0], "'title' field should exist in the data"


# Test the transformation of pricing data
def test_transform_data():
    sample_data = [
        {'id': 1, 'title': 'Product 1', 'price': 19.99, 'category': 'Category 1'},
        {'id': 2, 'title': 'Product 2', 'price': 25.50, 'category': 'Category 2'}
    ]
    df = transform_data(sample_data)
    assert isinstance(df, pd.DataFrame), "Transformed data should be a DataFrame"
    assert 'price_difference' in df.columns, "'price_difference' column should exist"
    assert df['price_difference'][0] == 19.99 - 20.0, "Price difference calculation is incorrect"
    assert df['price'][0] == 19.99, "Price transformation is incorrect"


# Test the loading of pricing data into SQLite
def test_load_data():
    df = pd.DataFrame({
        'id': [1, 2],
        'title': ['Product 1', 'Product 2'],
        'price': [19.99, 25.50],
        'category': ['Category 1', 'Category 2'],
        'price_difference': [-0.01, 5.50]
    })

    # Assuming the 'products' table exists in SQLite
    load_data(df)

    # Check if data is loaded correctly by querying the SQLite database
    conn = sqlite3.connect('pricing_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    count = cursor.fetchone()[0]
    conn.close()

    assert count > 0, "Data should be loaded into SQLite"
