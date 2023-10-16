"""
Tidy and prepare the Hansard data downloaded using download_hansard_transcripts.py

Requirements:

pip install --upgrade lxml

Usage:

python tidy_hansard.py

"""
import sqlite3
import zipfile

from lxml import etree

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
    # speech elements, then backfill the expected structure - we need
    # to do some grunt work to handle all of the edge cases though.
    for speech_number, speech in enumerate(root.xpath("//speech")):
        # First work out the debate this speech is part of.
        debate_info = ["", "", ""]

        parent = speech.getparent()

        # Ascend the tree until we can fill in all of the details
        while parent.tag not in ("debate", "petition.group"):
            # Note that not all petitions have speeches associated -
            # some are presented as the text of the petition without
            # a member speaking to them. This structure only catches
            # the petitions that have speeches
            if parent.tag in "petition":
                debate_info[1] = parent.find("petitioninfo").find("title").text

            if parent.tag in ("subdebate.1", "subdebate.2"):
                debate_info_level = 1 if parent.tag == "subdebate.1" else 2

                subdebateinfo = parent.find("subdebateinfo")

                # Edge case 'hansard80/hansardr80/1980-09-17', '2021-08-03 00:00:00'
                if subdebateinfo is None:
                    subdebateinfo = parent.find("debateinfo")

                title = subdebateinfo.find("title")

                if title:
                    debate_info[debate_info_level] = title.text
                # Edge case for 'hansard80/hansards80/1979-04-05', '2021-08-10 00:00:00'
                elif subdebateinfo.find("para") is not None:
                    debate_info[debate_info_level] = subdebateinfo.find("para").text
                # 'chamber/hansards/2007-05-10'
                else:
                    debate_info[debate_info_level] = "sub-debate"

            parent = parent.getparent()

        if parent.tag == "debate":
            # 2010-05-19 00:00:00 - no title for the debate
            title = parent.find("debateinfo").find("title")
            if title:
                debate_info[0] = title.text
            else:
                debate_info[0] = "debate"
        else:
            debate_info[0] = parent.find("petition.groupinfo").find("title").text

        debate_row = (None, transcript_id, date, house, *debate_info)
        debates.add(debate_row)
        speeches.append(
            [
                *debate_row,
                speech_number,
                None,
                etree.tostring(speech, with_tail=False),
            ]
        )

    return debates, speeches


def tidy_hansard(db_path="hansard.db", transcript_zip_path="hansard_transcripts.zip"):
    db_conn = sqlite3.connect(db_path, isolation_level=None)

    # TODO: how do we handle changes in schema?
    # Drop and replace everything probably?
    # TODO: how do we handle incremental updates?
    # We probably don't want to reprocess everything for a single new transcript.
    # TODO: add indexes to support common queries and incremental updates.
    schema_script = """
    pragma foreign_keys=0;

    drop table if exists speaker;
    create table speaker (
        -- Hansard data assigned
        speaker_id primary key
        -- TODO: what else goes here?
    );

    drop table if exists debate;
    create table debate (
        debate_id integer primary key,
        transcript_id references transcript on delete cascade,
        date,
        house,
        debate,
        subdebate_1,
        subdebate_2,
        unique (date, house, debate, subdebate_1, subdebate_2)
    );

    drop table if exists speech;
    create table speech (
        speech_id integer primary key,
        transcript_id references transcript on delete cascade,
        debate_id not null references debate,
        date,
        house,
        speech_number integer,
        main_speaker_id,
        speech_xml,
        unique(date, house, speech_number)
    );

    drop table if exists speech_turn;
    create table speech_turn (
        speech_id integer references speech on delete cascade,
        turn_number integer,
        speaker_id,
        raw_xml,
        plain_text,
        interjection bool,
        primary key (speech_id, turn_number)
    );

    pragma foreign_keys=1;
    """

    db_conn.executescript(schema_script)

    to_process = list(
        db_conn.execute(
            """
            select
                transcript_id, last_mod
            from transcript
            where xml_url is not null
                and access_time is not null
            """
        )
    )

    with zipfile.ZipFile(transcript_zip_path, "r") as transcripts:
        all_paths = set()

        db_conn.execute("begin")

        for i, (transcript_id, last_mod) in enumerate(to_process):
            # Skip the senate transcript duplicated into the HoR.
            # TODO: double check if there's actually a HoR sitting for that day?
            if transcript_id in skip_transcripts:
                print(
                    f"Skipping marked transcript - {transcript_id}. "
                    f"Reason: {skip_transcripts[transcript_id]}"
                )
                continue
            print(transcript_id, f"{i + 1}/{len(to_process)}")
            with transcripts.open(f"{transcript_id}/{last_mod}") as transcript_data:
                transcript_xml = transcript_data.read()

            debates, speeches = extract_speeches(transcript_id, transcript_xml)

            for debate in debates:
                db_conn.execute(
                    "insert into debate values(?, ?, ?, ?, ?, ?, ?)",
                    debate,
                )

            for speech in speeches:
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

        db_conn.execute("commit")

        for path in sorted(all_paths):
            print(path)

    db_conn.close()


if __name__ == "__main__":
    tidy_hansard()
