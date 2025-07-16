import pytest
import requests
import pandas as pd
from etl_pipeline import extract_data, transform_data, load_data
import sqlite3


# Test the extraction function
def test_extract_data():
    data = extract_data()
    assert data is not None, "Extracted data should not be None"
    assert len(data) > 0, "Extracted data should have more than 0 records"


# Test the transformation function
def test_transform_data():
    sample_data = [
        {'id': 1, 'title': 'Title 1', 'body': 'Body 1'},
        {'id': 2, 'title': 'Title 2', 'body': 'Body 2'}
    ]
    df = transform_data(sample_data)
    assert isinstance(df, pd.DataFrame), "Transformed data should be a DataFrame"
    assert 'id' in df.columns, "'id' column should exist"
    assert 'title' in df.columns, "'title' column should exist"
    assert 'body' in df.columns, "'body' column should exist"


# Test the load function
def test_load_data():
    df = pd.DataFrame({
        'id': [1, 2],
        'title': ['Title 1', 'Title 2'],
        'body': ['Body 1', 'Body 2']
    })

    # Assuming the 'posts' table exists in SQLite
    load_data(df)

    # Check if data is loaded correctly by querying the SQLite database
    conn = sqlite3.connect('etl_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM posts")
    count = cursor.fetchone()[0]
    conn.close()

    assert count > 0, "Data should be loaded into SQLite"

