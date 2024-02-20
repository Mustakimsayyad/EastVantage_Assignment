import sqlite3
import pandas as pd

conn = sqlite3.connect('Data_Engineer_ETL_Assignment.db')

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

# Converting quantity to int and removing decimal
grouped_df['quantity'] = grouped_df['quantity'].astype(int)

# Saving the results to a CSV file
grouped_df.to_csv('output.csv', sep=';', index=False)
conn.close()


sql code 
# Reading data from tables
use database Data_Engineer_ETL_Assignment;
SELECT * FROM customers;
SELECT * FROM sales;
SELECT * FROM orders;
SELECT * FROM items;

# Merging tables
SELECT *
FROM orders
JOIN items ON orders.item_id = items.item_id
JOIN sales ON orders.sales_id = sales.sales_id
JOIN customers ON orders.customer_id = customers.customer_id;

# Filtering data for customers aged 18-35
SELECT *
FROM (
    SELECT *
    FROM orders
    JOIN items ON orders.item_id = items.item_id
    JOIN sales ON orders.sales_id = sales.sales_id
    JOIN customers ON orders.customer_id = customers.customer_id
) AS merged
WHERE merged.age BETWEEN 18 AND 35 AND merged.quantity > 0;

# Aggregate total quantities per customer and item
SELECT age, item_name, SUM(quantity) AS total_quantity
FROM (
    SELECT *
    FROM orders
    JOIN items ON orders.item_id = items.item_id
    JOIN sales ON orders.sales_id = sales.sales_id
    JOIN customers ON orders.customer_id = customers.customer_id
) AS merged
WHERE merged.age BETWEEN 18 AND 35 AND merged.quantity > 0
GROUP BY age, item_name;
