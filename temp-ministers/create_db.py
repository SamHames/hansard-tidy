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



"""
Proposed DB schema for hansard project

# role
# role_id
# role_name: member, minister, etc


# speaker role
# speaker_role_id (primary key)
# speaker_id (foreign key linking speaker)
# role_id (foreign key linking role)
# start date for each role
# end date for each role
many to many relationship


# subdebate
# subdebate_id (primary key)
# speech_id (foreign key linking to speech)
# title
# type e.g legislation, question time, etc

  
# subdebate text
# text_id (primary key)
# subdebdate_id (foreign key linking to subdebate)
# plain_text


# parliament_session
# session_id (primary key)
# parliament_no
# session_no
# date


# parliament_member (information on all members of the nth parliament)
# could be moved to another file? not sure how the process would work
# member_id (primary key)
# session_id (foreign key linking to session_id)
# roles etc
"""