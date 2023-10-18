"""
Prepare data live from parlinfo website.

Running this script will download and prepare an up to date index of all
Hansard transcript files.

Needed dependencies:

pip install --upgrade lxml requests site-map-parser

Usage:

python download_hansard_transcripts.py

"""

import collections
from datetime import datetime, timezone, timedelta
import sqlite3
import time
import urllib.parse
import zipfile

from lxml import html
import requests
from requests.adapters import HTTPAdapter, Retry
from sitemapparser import SiteMapParser


session = requests.Session()
retries = Retry(total=5, backoff_factor=1)
session.mount("http://", HTTPAdapter(max_retries=retries))


def download_all_transcripts(
    db_path="hansard.db", transcript_zip_path="hansard_transcripts.zip"
):
    db = sqlite3.connect(db_path, isolation_level=None)

    db.executescript(
        """
        -- This part of the process shouldn't affect already processed data.
        pragma foreign_keys=0;
        pragma journal_mode=WAL;

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

    print("Checking sitemaps for updated transcripts.")

    for i, sitemap in enumerate(all_maps):
        print(f"{sitemap} - {i+1} / up to {len(all_maps)}")
        subsite = SiteMapParser(sitemap)
        subsite_urls = list(subsite.get_urls())

        # Confirm that we are processing sitemaps in descending order of lastmod.
        # If this isn't true the early termination check won't work...
        lastmod = max(url.lastmod for url in subsite_urls)
        assert lastmod <= prev_lastmod
        prev_lastmod = lastmod

        if lastmod < check_until:
            break

        for url in subsite.get_urls():
            if "hansard" in url.loc:
                # Parse and extract the query string which includes the query for
                # the specific item/fragment from the Hansard to be retrieved.
                # Then keep the version/url of each fragment that has the most
                # recent lastmod as the indicator of the freshness of the
                # transcript corresponding to those fragments.
                parsed = urllib.parse.urlparse(url.loc).params
                queried_path = urllib.parse.parse_qs(parsed)["query"][0].split(";")[0][
                    4:
                ]
                group = "/".join(queried_path.split("/")[:3])
                # We want to keep: the earliest fragment of the group, and the
                # latest lastmod date.
                if group in activity_latest:
                    lastmod, loc = activity_latest[group]
                    activity_latest[group] = (
                        max(lastmod, url.lastmod),
                        min(loc, url.loc),
                    )
                else:
                    activity_latest[group] = (url.lastmod, url.loc)

        time.sleep(1)

    # Compare the expected transcripts to the previously seen versions, and work
    # out if any have changed using lastmod. For the ones that have changed, find
    # the corresponding XML transcript, if it exists.
    db.execute("begin")
    db.execute(
        """
        create temporary table active_transcript(
            transcript_id primary key,
            last_mod not null,
            html_url
        )
        """
    )

    db.executemany(
        "insert into active_transcript values(?, ?, ?)",
        ((transcript, *details) for transcript, details in activity_latest.items()),
    )

    # Note that the replace into here will delete and reinsert transcript
    # rows - when we get to processing the full scripts, this will cause
    # cascading deletes of out of date data extracted from these transcripts.
    # TODO: Figure out what indexes we need to make this work effectively
    # when we get to the incremental handling case.
    db.execute(
        """
        replace into transcript(transcript_id, last_mod, html_url)
        select
            transcript_id,
            at.last_mod,
            at.html_url
        from active_transcript at
        left outer join transcript t using(transcript_id)
        where t.last_mod is null
            or at.last_mod > t.last_mod
        """
    )

    db.execute(
        "replace into metadata values('last-run', (select max(last_mod) from transcript))"
    )

    db.execute("commit")

    # Note - we're back in autocommit mode, as we'll be doing infrequent single
    # row updates to track files as they're updated.
    to_download = list(
        db.execute(
            """
            select
                transcript_id,
                last_mod,
                html_url
            from transcript
            where access_time is null
            """
        )
    )

    # TODO: work out how to handle rebuilding the zip file with only the current
    # versions of transcripts.
    with zipfile.ZipFile(
        transcript_zip_path, "a", zipfile.ZIP_DEFLATED
    ) as transcript_zip:
        for i, (transcript_id, lastmod, url) in enumerate(to_download):
            print(f"Downloading {transcript_id}, {i+1}/{len(to_download)}")
            response = session.get(url)
            response.raise_for_status()
            transcript_page = html.fromstring(response.content)
            transcript_page.make_links_absolute("https://parlinfo.aph.gov.au/")

            # Find the link to the XML version of the transcript for this page.
            xml_transcript_links = [
                url
                for url in transcript_page.xpath("//a/@href")
                if "/toc_unixml/" in url
            ]
            assert len(xml_transcript_links) <= 1

            if xml_transcript_links:
                xml_url = xml_transcript_links[0]
                response = session.get(xml_url)
                response.raise_for_status()
                transcript_zip.writestr(
                    f"{transcript_id}/{lastmod}",
                    response.content,
                )
            else:
                xml_url = None

            db.execute(
                """
                update transcript set
                xml_url = ?,
                access_time = ?
                where transcript_id = ?
                """,
                [xml_url, datetime.now(tz=timezone.utc), transcript_id],
            )

            time.sleep(1)

    db.close()


if __name__ == "__main__":
    download_all_transcripts()
