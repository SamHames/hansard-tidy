import sqlite3
import json

# Create SQLite database file
conn = sqlite3.connect('67.db')

# Create table
schema_script = """
pragma foreign_keys=1;

create table if not exists ministers (
    id INTEGER primary key autoincrement,
    MID INTEGER,
    PHID TEXT,
    DisplayName TEXT,
    Role TEXT,
    Prep TEXT,
    Entity TEXT,
    Gender TEXT,
    RDateStart TEXT,
    RDateEnd TEXT,
    Ministry TEXT,
    MDateStart TEXT,
    MDateEnd TEXT,
    MPorSenator TEXT
);
"""

conn.executescript(schema_script)

with open("67.json", 'r') as file:
    data = json.load(file)
    json_data = data['value']


# Insert JSON data into the SQLite database
for record in json_data:
    conn.execute('''
    INSERT INTO ministers (
        MID, PHID, DisplayName, Role, Prep, Entity, Gender,
        RDateStart, RDateEnd, Ministry, MDateStart, MDateEnd, MPorSenator
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    ''', (
        record['MID'], record['PHID'], record['DisplayName'], record['Role'],
        record['Prep'], record['Entity'], record['Gender'],
        record['RDateStart'], record['RDateEnd'], record['Ministry'],
        record['MDateStart'], record['MDateEnd'], record['MPorSenator']
    ))
    conn.commit()

# Close the database connection
conn.close()
