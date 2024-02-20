import sqlite3
import pandas as pd

conn = sqlite3.connect('Data_Engineer_ETL_Assignment.db.db')

# Reading data from tables
customers = pd.read_sql_query("SELECT * FROM customers", conn)
sales = pd.read_sql_query("SELECT * FROM sales", conn)
orders = pd.read_sql_query("SELECT * FROM orders", conn)
items = pd.read_sql_query("SELECT * FROM items", conn)

# Merging tables 
df = pd.merge(orders, items, on='item_id')
df = pd.merge(df, sales, on='sales_id')
df = pd.merge(df, customers, on='customer_id')

# Filtering data for customers aged 18-35
filtered_df = df[(df['age'] >= 18) & (df['age'] <= 35) & (df['quantity'] > 0)]

# Aggregate total quantities per customer and item
grouped_df = filtered_df.groupby(['age', 'item_name'])['quantity'].sum().reset_index()
