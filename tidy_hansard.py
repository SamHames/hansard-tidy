import sqlite3


db_conn = sqlite3.connect("tidy_hansard.db")


# TODO: how do we handle changes in schema?
# Drop and replace everything probably?
schema_script = """
pragma foreign_keys=1;

drop table if exists transcript;
create table transcript (
    house,
    date,
    url,
    primary key (house, date)
);

drop table if exists speaker;
create table speaker (
    -- Hansard data assigned
    speaker_id primary key
    -- TODO: what else goes here?
);

drop table if exists speech;
create table speech (
    speech_id integer primary key,
    house,
    date,
    speech_number integer,
    main_speaker_id,
    unique(date, house, speech_number)
);

drop table if exists speech_turn;
create table speech_turn (
    speech_id integer references speech,
    turn_number integer,
    speaker_id,
    raw_xml,
    plain_text,
    interjection bool,
    primary key (speech_id, turn_number)
);
"""

db_conn.executescript(schema_script)
