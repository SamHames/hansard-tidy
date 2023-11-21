"""
Download all HTML fragments of the Hansard transcripts.

We use the HTML transcripts for this instead of the XML transcripts because:

1. It's easier to deeplink to the HTML fragments and the official record.
2. The HTML transcripts cover the period 1981-1998 as well, unlike the XML.

Needed dependencies:

pip install --upgrade lxml requests site-map-parser

Usage:

python download_hansard_html.py

"""

import collections
from datetime import datetime, timezone, timedelta
import sqlite3
import time
import urllib.parse
import zipfile

import requests
from requests.adapters import HTTPAdapter, Retry
from sitemapparser import SiteMapParser


session = requests.Session()
retries = Retry(total=30, backoff_factor=1, backoff_max=60)

session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))


def download_all_html(db_path="hansard_html.db", html_zip_path="hansard_html.zip"):
    db = sqlite3.connect(db_path, isolation_level=None)

    db.executescript(
        """
        -- This part of the process shouldn't affect already processed data.
        pragma foreign_keys=0;
        pragma journal_mode=WAL;

        create table if not exists proceedings_page (
            /*
            This table acts as the driver for what work needs to be done to
            keep the collection fresh, and also acts as the index into the
            zipfile of raw HTML.

            */
            -- Generate an integer primary key for this page as a key
            -- into the zipfile.
            page_id integer primary key,
            url unique,
            last_mod not null,
            -- The time the transcript was retrieved, null if not yet retrieved.
            access_time,
            -- The time the transcript was processed into the tidy schema.
            -- Null indicates that the work is still outstanding.
            process_time
        );

        -- Used for finding updated HTML fragments.
        create temporary table active_proceedings(
            url primary key,
            last_mod not null
        );

        create table if not exists metadata (
            key primary key,
            value
        );

        insert or ignore into metadata values('last-run', '1899-01-01 00:00:00')

        """
    )

    # We overshoot by quite a bit, just to make sure we don't have to worry
    # about boundary effects.
    check_until = datetime.fromisoformat(
        list(db.execute("select value from metadata where key = 'last-run'"))[0][0]
    ) - timedelta(weeks=4)

    # Part 1: Generate the index of all siting days, by the date they were last modified.
    sm = SiteMapParser("https://parlinfo.aph.gov.au/sitemap/sitemapindex.xml")

    # Reversed, because we want to check the most recently update sitemap first so
    # we can terminate early if needed. Only the first complete run of this
    # should be intensive.
    all_maps = list(reversed(list(sm.get_sitemaps())))

    # The sitemap URLs all point to specific fragments of each proceedings. We're
    # aiming to find the fragments with the latest modification date, as a proxy
    # for the sitting days that have been updated since the latest run.
    activity_latest = dict()
    prev_lastmod = datetime.now()

    print("Checking sitemaps for updated fragments.")

    for i, sitemap in enumerate(all_maps):
        print(f"{sitemap} - {i+1} / up to {len(all_maps)}")
        attempts = 0
        while True:
            try:
                subsite = SiteMapParser(sitemap)
                break
            except Exception:
                delay = min(60, 2**attempts)
                print(f"Caught exception, retrying in {delay}")
                time.sleep(delay)
                attempts += 1

                if attempts >= 30:
                    raise

        subsite_urls = list(subsite.get_urls())

        # Confirm that we are processing sitemaps in descending order of lastmod.
        # If this isn't true the early termination check won't work...
        lastmod = min(url.lastmod for url in subsite_urls)
        assert lastmod <= prev_lastmod
        prev_lastmod = lastmod

        if lastmod < check_until:
            break

        hansard_urls = [
            (url.loc, url.lastmod) for url in subsite.get_urls() if "hansard" in url.loc
        ]

        # Mark updated hansard URLs
        db.execute("delete from active_proceedings")
        db.executemany("insert into active_proceedings values (?, ?)", hansard_urls)
        db.execute(
            """
            replace into proceedings_page(page_id, url, last_mod)
            select
                pp.page_id,
                ap.url,
                ap.last_mod
            from active_proceedings ap
            left outer join proceedings_page pp using(url)
            where pp.last_mod is null
                or ap.last_mod > pp.last_mod
            """
        )

    # Note that the replace into here will delete and reinsert transcript
    # rows, but because foreign_keys are off won't otherwise change any other
    # table.
    db.execute(
        "replace into metadata values('last-run', (select max(last_mod) from proceedings_page))"
    )

    # Note - we're back in autocommit mode, as we'll be doing infrequent single
    # row updates to track files as they're updated.
    to_download = list(
        db.execute(
            """
            select
                page_id,
                url,
                last_mod
            from proceedings_page
            where access_time is null
            """
        )
    )

    # TODO: work out how to handle rebuilding the zip file with only the current
    # versions of transcripts.
    with zipfile.ZipFile(html_zip_path, "a", zipfile.ZIP_DEFLATED) as html_zip:
        for i, (page_id, url, last_mod) in enumerate(to_download):
            start = time.monotonic()
            destination = f"{page_id}/{last_mod}"

            print(f"Downloading {url} into {destination}, {i+1}/{len(to_download)}")
            response = session.get(url, timeout=5)
            response.raise_for_status()

            html_zip.writestr(
                destination,
                response.text,
            )

            db.execute(
                """
                update proceedings_page set
                    access_time = ?
                where url = ?
                """,
                [datetime.now(tz=timezone.utc), url],
            )

            # Make no more than 2 requests per second.
            taken = time.monotonic() - start
            delay = max(0.5 - taken, 0)
            wait = time.sleep(delay)

    db.close()


if __name__ == "__main__":
    download_all_html()
