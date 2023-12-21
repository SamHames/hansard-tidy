"""
Tidy and prepare the Hansard data downloaded using download_hansard_transcripts.py

Requirements:

pip install --upgrade lxml tqdm

Usage:

python tidy_hansard.py

"""
import concurrent.futures as cf
from datetime import datetime
from html import unescape
import os
import random
import sqlite3
import zlib

from lxml import html
from tqdm import tqdm


schema_statements = [
    """
    create table debate (
        debate_id integer primary key,
        date datetime not null,
        house text not null,
        parl_no integer not null,
        title text not null,
        unique (date, house, title)
    )
    """,
    """
    create table proceedings_page (
        page_id integer primary key,
        url unique not null,
        access_time not null,
        date datetime not null,
        house text not null,
        parl_no integer not null,
        debate_id integer references debate not null,
        speech_html text not null
    )
    """,
    """
    create table metadata (
        page_id integer references proceedings_page,
        key not null,
        value text,
        primary key (page_id, key)
    )
    """,
    """
    create table failed_processing_page (
        url primary key,
        access_time not null
    )
    """,
    "pragma schema_version=1",
]


def extract_page_data(url, access_time, compressed_page):
    """Extract just the metadata keys from the relevant page heading."""

    try:
        html_page = zlib.decompress(compressed_page)

        root = html.fromstring(html_page)
        root.make_links_absolute("https://parlinfo.aph.gov.au")

        content_comp = root.xpath("//div[@id='documentContentPanel']")
        content = html.tostring(content_comp[0], with_tail=False, encoding="unicode")

        metadata_block = root.xpath("//div[@class='metadata']")[0]

        # Extract the metadata tags from the relevant section by
        # reassembling the definition list.
        dts = metadata_block.xpath(".//dt")
        dds = metadata_block.xpath(".//dd")

        key_values = (
            # Sometimes the empty values are filled with the HTML escape
            # &nbsp, sometimes they're filled with the '\xa0' character...
            (key.text, unescape("".join(value.itertext())).strip())
            for key, value in zip(dts, dds)
        )

        metadata = {key: value for key, value in key_values if value}

        # Title doesn't always exist, make sure it's there.
        metadata["Title"] = metadata.get("Title", "")
        metadata["Date"] = datetime.strptime(metadata["Date"], "%d-%m-%Y").date()
        metadata["Parl No."] = int(metadata["Parl No."])

    except Exception:
        return url, access_time, set(), None

    return url, access_time, metadata, content


def insert_data(db_conn, result):
    url, access_time = result[:2]
    metadata, content = result[2:]

    if content is None:
        db_conn.execute(
            "insert into main.failed_processing_page values (?, ?)",
            (url, access_time),
        )
        failures += 1
    else:
        # Make sure debate row exists
        row_meta = [
            metadata[key]
            for key in [
                "Date",
                "Database",
                "Parl No.",
                "Title",
            ]
        ]
        db_conn.execute(
            "insert or ignore into debate values(?, ?, ?, ?, ?)",
            (
                None,
                *row_meta,
            ),
        )

        row_meta = [
            metadata[key]
            for key in [
                "Date",
                "Database",
                "Title",
            ]
        ]
        debate_id = list(
            db_conn.execute(
                "select debate_id from debate where (date, house, title) = (?, ?, ?)",
                row_meta,
            )
        )[0][0]

        # Speech content
        row_meta = [
            metadata[key]
            for key in [
                "Date",
                "Database",
                "Parl No.",
            ]
        ]
        db_conn.execute(
            "insert into main.proceedings_page values (?, ?, ?, ?, ?, ?, ?, ?)",
            (None, url, access_time, *row_meta, debate_id, content),
        )
        page_id = list(db_conn.execute("select last_insert_rowid()"))[0][0]

        # All metadata for reference.
        db_conn.executemany(
            "insert into metadata values (?, ?, ?)",
            ((page_id, key, value) for key, value in metadata.items()),
        )


def tidy_hansard(source_db="hansard_html.db", target_db="tidy_hansard2.db"):
    """ """

    if os.path.exists(target_db):
        raise ValueError(
            "Target DB must not already exist - "
            "delete the file or select a new target."
        )

    db_conn = sqlite3.connect(target_db)

    db_conn.execute("attach ? as collected", [source_db])

    db_conn.execute("begin")

    # Setup the schema.
    for statement in schema_statements:
        db_conn.execute(statement)

    to_process = list(
        db_conn.execute(
            """
            select count(*)
            from collected.proceedings_page
            where access_time is not null
            """
        )
    )[0][0]

    completed = 0
    failures = 0
    futures = set()

    with cf.ProcessPoolExecutor() as pool:
        for row in tqdm(
            db_conn.execute(
                """
                select
                    url, access_time, compressed_page
                from collected.proceedings_page
                where access_time is not null
                order by url
                """
            ),
            total=to_process,
            smoothing=0.01,
        ):
            if random.random() <= 1.0:
                futures.add(pool.submit(extract_page_data, *row))

            if len(futures) >= 1000:
                done, futures = cf.wait(futures, return_when="FIRST_COMPLETED")

                for future in done:
                    result = future.result()
                    insert_data(db_conn, result)

        for future in cf.as_completed(futures):
            insert_data(db_conn, result)

    db_conn.execute("commit")


if __name__ == "__main__":
    tidy_hansard()
