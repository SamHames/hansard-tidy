"""
Tidy and prepare the Hansard data downloaded using download_hansard_transcripts.py

Requirements:

pip install --upgrade lxml

Usage:

python tidy_hansard.py

"""
from datetime import datetime
import os
import sqlite3
import tempfile
import zipfile

from lxml import etree
import requests


skip_transcripts = {
    "hansard80/hansardr80/1979-02-20": "Actually a senate transcript, duplicate of hansard80/hansards80/1979-02-20",
    "hansard80/hansardr80/1973-05-09": "Actually hansard80/hansardr80/1973-05-29",
    "hansard80/hansardr80/19111204": "Duplicate of hansard80/hansardr80/1911-12-04",
    "hansard80/hansardr80/19480908": "Duplicate of hansard80/hansardr80/1948-09-08",
    "chamber/hansardr/1901-10-16": "Duplicate of hansard80/hansardr80/1901-10-16",
    "chamber/hansards/2010-02-23": "Duplicate of chamber/hansards/2010-03-09",
}


def extract_speeches(transcript_id, transcript_xml):
    root = etree.fromstring(transcript_xml)

    session = root.find("session.header")
    house = session.find("chamber").text

    # Normalise houses
    if house == "REPS":
        house = "House of Reps"
    elif house in ("SENATE", "SEN"):
        house = "Senate"

    date = session.find("date").text

    # Special case - the date appears to be inconsistent in the file.
    if transcript_id == "chamber/hansardr/2009-06-03":
        date = "2009-06-03"

    debates = set()
    speeches = []

    # Find speech elements at any level - the strategy is to find all
    # speech-like elements, then backfill the expected structure - we need to
    # do some grunt work to handle all of the edge cases though. Note that we
    # also treat questions and answers as speeches.
    speech_nodes = root.xpath("//speech|//question|//quest|//quesion|//answer")

    for speech_number, speech in enumerate(speech_nodes):
        # First work out the debate this speech is part of.

        # Defaults to handle various edge cases
        # 'chamber/hansards/2007-05-10'
        debate_info = [
            "<untitled debate>",
            "",
            "",
        ]

        parent = speech.getparent()

        # Ascend the tree until we can fill in all of the details. Sometimes
        # there can be interposing structure that obscures the debate, so we
        # just walk up the tree until we hit the right elements.
        skip = False
        while parent is not None and parent.tag not in ("debate", "petition.group"):
            # If we hit one of the other container tags in the parent, this is
            # the wrong element to be the "speech". Note that there can be
            # complex nesting - an answer tag might contain a speech tag from
            # someone else.
            if parent.tag in ("speech", "question", "answer", "quest", "quesion"):
                skip = True
                break

            # Note that not all petitions have speeches associated -
            # some are presented as the text of the petition without
            # a member speaking to them. This structure only catches
            # the petitions that have speeches
            if parent.tag == "petition":
                debate_info[1] = parent.find("petitioninfo").find("title").text

            if parent.tag in ("subdebate.1", "subdebate.2"):
                debate_info_level = 1 if parent.tag == "subdebate.1" else 2

                # Edge case 'hansard80/hansardr80/1980-09-17', '2021-08-03 00:00:00'
                if parent.find("subdebateinfo") is not None:
                    subdebateinfo = parent.find("subdebateinfo")
                elif parent.find("debateinfo") is not None:
                    subdebateinfo = parent.find("debateinfo")

                title = subdebateinfo.find("title")

                if title is not None:
                    if title.text:
                        debate_info[debate_info_level] = title.text
                    else:
                        debate_info[debate_info_level] = "<untitled sub-debate>"
                # Edge case for 'hansard80/hansards80/1979-04-05', '2021-08-10 00:00:00'
                elif subdebateinfo.find("para") is not None:
                    debate_info[debate_info_level] = subdebateinfo.find("para").text

            parent = parent.getparent()

        if skip:
            continue

        if parent is not None and parent.tag == "debate":
            deb_info = parent.find("debateinfo")
            if deb_info is not None:
                title = deb_info.find("title")
                if title is not None and title.text:
                    debate_info[0] = title.text

        elif parent is not None:
            debate_info[0] = parent.find("petition.groupinfo").find("title").text

        if None in debate_info:
            breakpoint()

        # Find all talkers and interjectors
        speakers = {
            speaker.text.lower()
            for speaker in speech.xpath(
                ".//talk.start[not(parent::interjection)]//name.id"
            )
            if speaker.text
        }

        interjectors = {
            speaker.text.lower().strip()
            for speaker in speech.xpath(".//interjection//talk.start//name.id")
            if speaker.text
        }

        debate_row = (None, transcript_id, date, house, *debate_info)
        debates.add(debate_row)

        speech_type = {
            "question": "question",
            "quest": "question",
            "quesion": "question",
            "answer": "answer",
            "speech": "speech",
        }[speech.tag]

        speeches.append(
            (
                (
                    *debate_row,
                    speech_number,
                    speech_type,
                    etree.tostring(speech, with_tail=False),
                ),
                speakers,
                interjectors,
            )
        )

    return debates, speeches


def tidy_hansard(
    db_path="hansard.db",
    transcript_zip_path="hansard_transcripts.zip",
    rebuild=True,
):
    if rebuild:
        # Copy over the important bits to a new database, then replace the old
        # database. This is faster than trying to empty out the database since
        # we know we're going to throw everything out anyway.
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, "temp.db")
        temp_conn = sqlite3.connect(temp_path, isolation_level=None)
        temp_conn.execute("attach ? as old", [db_path])

        temp_conn.executescript(
            """
            create table if not exists transcript (
                /*
                This table acts as the driver for what work needs to be done to keep
                the collection fresh, and also acts as the index into the zipfile of
                raw transcripts.

                The transcript_id is also the path into the zipfile for this
                transcript.

                */
                transcript_id primary key,
                html_url,
                -- This is not required to be present - 1981-1998 don't have XML transcripts.
                xml_url,
                last_mod not null,
                -- The time the transcript was retrieved, null if not yet retrieved.
                access_time,
                -- The time the transcript was processed into the tidy schema.
                -- Null indicates that the work is still outstanding.
                process_time
            );

            insert into main.transcript select * from old.transcript;
            update transcript set process_time = null;

            create table if not exists metadata (
                key primary key,
                value
            );

            insert into main.metadata select * from old.metadata;
            """
        )
        temp_conn.close()
        os.rename(temp_path, db_path)

    db_conn = sqlite3.connect(db_path, isolation_level=None)

    # TODO: how do we handle changes in schema?
    # Drop and replace everything probably?
    # TODO: how do we handle incremental updates?
    # We probably don't want to reprocess everything for a single new transcript.
    # TODO: add indexes to support common queries and incremental updates.
    schema_script = """
        create table if not exists member (
            -- Hansard data assigned
            phid primary key,
            name,
            gender,
            latest_state,
            latest_electorate,
            latest_party,
            date_of_birth
            -- TODO: what else goes here?
        );

        create table if not exists debate (
            debate_id integer primary key,
            transcript_id references transcript on delete cascade,
            date,
            house,
            debate,
            subdebate_1,
            subdebate_2,
            unique (date, house, debate, subdebate_1, subdebate_2)
        );

        create index if not exists transcript_debate on debate(transcript_id);

        create table if not exists speech (
            speech_id integer primary key,
            transcript_id references transcript on delete cascade,
            debate_id not null references debate,
            date,
            house,
            speech_number integer,
            speech_type text,
            speech_xml,
            unique(date, house, speech_number)
        );

        create index if not exists transcript_speech on speech(transcript_id);
        create index if not exists debate_speech on speech(debate_id);

        create table if not exists speech_speaker (
            speech_id integer references speech(speech_id) on delete cascade,
            phid references member(phid),
            primary key (speech_id, phid)
        );
        create index if not exists speaker_speech on speech_speaker(phid, speech_id);

        create table if not exists speech_interjector (
            speech_id integer references speech(speech_id) on delete cascade,
            phid references member(phid),
            primary key (speech_id, phid)
        );
        create index if not exists interjector_speech on speech_interjector(
            phid, speech_id
        );

        create table if not exists speech_turn (
            speech_id integer references speech on delete cascade,
            turn_number integer,
            phid,
            raw_xml,
            plain_text,
            interjection bool,
            primary key (speech_id, turn_number)
        );
    """

    db_conn.executescript(schema_script)

    db_conn.execute("begin")

    # Replace the members table
    db_conn.execute("delete from member")
    member_data = requests.get(
        "https://handbookapi.aph.gov.au/api/"
        "individuals?$orderby=FamilyName,GivenName&"
        "$skip=0&$count=true&"
        "$select=PHID,DisplayName,Gender,State,"
        "Electorate,Party,DateOfBirth"
    )

    for member in member_data.json()["value"]:
        values = [
            member[key]
            for key in "PHID,DisplayName,Gender,State,Electorate,Party,DateOfBirth".split(
                ","
            )
        ]
        values[0] = values[0].lower()
        db_conn.execute("insert into member values(?, ?, ?, ?, ?, ?, ?)", values)

    db_conn.execute("pragma foreign_keys=1")
    # Delete outdated transcript rows by following the foreign key
    # relationships
    db_conn.execute(
        """
        -- A replace is a delete followed by an insert - the delete
        -- triggers the foreign key cascade.
        replace into transcript
        select *
        from transcript
        where xml_url is not null
            and access_time is not null
            and process_time is null
        """
    )

    to_process = list(
        db_conn.execute(
            """
            select
                transcript_id, last_mod
            from transcript
            where xml_url is not null
                and access_time is not null
                and process_time is null
            """
        )
    )

    all_members = {row[0] for row in db_conn.execute("select phid from member")}

    with zipfile.ZipFile(transcript_zip_path, "r") as transcripts:
        for i, (transcript_id, last_mod) in enumerate(to_process):
            # Skip the senate transcript duplicated into the HoR.
            # TODO: double check if there's actually a HoR sitting for that day?
            if transcript_id in skip_transcripts:
                print(
                    f"Skipping marked transcript - {transcript_id}. "
                    f"Reason: {skip_transcripts[transcript_id]}"
                )
                continue
            # print(transcript_id, f"{i + 1}/{len(to_process)}")
            with transcripts.open(f"{transcript_id}/{last_mod}") as transcript_data:
                transcript_xml = transcript_data.read()

            debates, speeches = extract_speeches(transcript_id, transcript_xml)

            for debate in debates:
                db_conn.execute(
                    "insert into debate values(?, ?, ?, ?, ?, ?, ?)",
                    debate,
                )

            for speech, speakers, interjectors in speeches:
                db_conn.execute(
                    """
                    insert into speech values(
                        ?1,
                        ?2,
                        (
                            select
                                debate_id
                            from debate
                            where (date, house, debate, subdebate_1, subdebate_2) =
                                (?3, ?4, ?5, ?6, ?7)
                        ),
                        ?3,
                        ?4,
                        ?8,
                        ?9,
                        ?10
                    )
                    """,
                    speech,
                )

                speech_id = list(db_conn.execute("select last_insert_rowid()"))[0][0]

                # Insert only the valid keys
                # TODO: figure out a strategy for handling the invalid member
                # ids and the role based IDs.
                db_conn.executemany(
                    "insert into speech_speaker values(?, ?)",
                    ((speech_id, phid) for phid in speakers & all_members),
                )
                db_conn.executemany(
                    "insert into speech_interjector values(?, ?)",
                    ((speech_id, phid) for phid in interjectors & all_members),
                )

            db_conn.execute(
                "update transcript set process_time = ? where transcript_id = ?",
                [datetime.now(), transcript_id],
            )

    db_conn.execute("commit")
    db_conn.execute("pragma journal_mode=WAL")
    db_conn.close()


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    rebuild = "rebuild" in args
    tidy_hansard(rebuild=rebuild)
