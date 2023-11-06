import sqlite3

# Connect to the database (this will create the database file if it does not exist)
conn = sqlite3.connect('67.db')

# Create a cursor object using the cursor method
cursor = conn.cursor()

# Select all rows from the table named 'your_table_name'
cursor.execute("SELECT * FROM ministers")

# Fetch all rows from the last executed statement
rows = cursor.fetchall()

# Print the rows
for row in rows:
    print(row)

# Close the connection
conn.close()
