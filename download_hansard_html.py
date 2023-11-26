"""
Download all HTML fragments of the Hansard transcripts.

We use the HTML transcripts for this instead of the XML transcripts because:

1. It's easier to deeplink to the HTML fragments and the official record.
2. The HTML transcripts cover the period 1981-1998 as well, unlike the XML.

Needed dependencies:

pip install --upgrade lxml requests

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
from lxml import etree

session = requests.Session()
retries = Retry(
    total=5, backoff_factor=1, backoff_max=60, status_forcelist=[500, 502, 503, 504]
)

session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))


def get_sitemap_urls(sitemap_loc):
    sitemap_ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    request = session.get(sitemap_loc)
    sitemap_map = etree.fromstring(request.content)

    return [
        (elem.find(sitemap_ns + "loc").text, elem.find(sitemap_ns + "lastmod").text)
        for elem in sitemap_map.findall(sitemap_ns + "sitemap")
    ]


def get_location_urls(sitemap_url):
    sitemap_ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    request = session.get(sitemap_url)
    sitemap_map = etree.fromstring(request.content)

    return [
        (elem.find(sitemap_ns + "loc").text, elem.find(sitemap_ns + "lastmod").text)
        for elem in sitemap_map.findall(sitemap_ns + "url")
    ]


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

        insert or ignore into metadata values('last-run', julianday('1899-01-01'));

        """
    )

    # Only update the list of URLs every 24 hours.
    time_since_last_sitemap_check = list(
        db.execute(
            "select julianday('now') - value from metadata where key = 'last-run'"
        )
    )[0][0]

    if time_since_last_sitemap_check > 1:
        all_sitemaps = get_sitemap_urls(
            "https://parlinfo.aph.gov.au/sitemap/sitemapindex.xml"
        )

        print("Checking sitemaps for updated fragments.")
        db.execute("begin")

        # Note that we check all sitemaps, because otherwise we can't tell
        # if urls have been deleted from the global list.
        for i, (sitemap, _) in enumerate(all_sitemaps):
            print(f"{sitemap} - {i+1} / up to {len(all_sitemaps)}")

            # Process all subsites, so we can confirm if a fragment is deleted.
            subsite_urls = get_location_urls(sitemap)
            hansard_urls = [
                (loc, lastmod) for loc, lastmod in subsite_urls if "hansard" in loc
            ]

            # Mark updated hansard URLs
            db.executemany("insert into active_proceedings values (?, ?)", hansard_urls)

        # 1. Handle deleted pages
        db.execute(
            """
            delete from proceedings_page
            where url not in (select url from active_proceedings)
            """
        )
        # 2. Mark updated pages for retrieval
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

        db.execute("replace into metadata values('last-run', julianday('now')")
        db.execute("commit")

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
            try:
                response = session.get(url, timeout=5)
                response.raise_for_status()
            except Exception:
                print(f"Skipping {url} due to error")
                continue

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

            # Make no more than 4 requests per second.
            taken = time.monotonic() - start
            delay = max(0.25 - taken, 0)
            wait = time.sleep(delay)

    db.close()


if __name__ == "__main__":
    download_all_html()
