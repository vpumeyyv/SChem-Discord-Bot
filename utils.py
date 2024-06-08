#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from datetime import datetime, timezone, timedelta


def split_by_char_limit(s, limit):
    """Given a string, return it split it on newlines into chunks under the given char limit.

    Raise an exception if a single line exceeds the char limit.

    max_chunk_size: Maximum size of individual strings. Default 1900 to fit comfortably under discord's 2000-char limit.
    """
    chunks = []

    while s:
        # Terminate if s is under the chunk size
        if len(s) <= limit:
            chunks.append(s)
            return chunks

        # Find the last newline before the chunk limit
        cut_idx = s.rfind("\n", 0, limit + 1)  # Look for the newline closest to the char limit
        if cut_idx == -1:
            raise ValueError(f"Can't split message with line > {limit} chars")

        chunks.append(s[:cut_idx])
        s = s[cut_idx + 1:]


def parse_datetime_str(s):
    """Parse and check validity of given ISO date string then return as a UTC Datetime (converting as needed)."""
    dt = datetime.fromisoformat(s.rstrip('Z'))  # For some reason isoformat doesn't like Z (Zulu time) suffix

    # If timezone unspecified, assume UTC, else convert to UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt


def process_start_end_dates(start, end, check_start_in_future=True):
    """Helper for validating and reformatting tournament start/end date args (e.g. tournament or round start & end).
    When updating an existing tournament, check for start date being in future should be ignored.
    """
    start_dt = parse_datetime_str(start)
    end_dt = parse_datetime_str(end)
    cur_dt = datetime.now(timezone.utc)

    if check_start_in_future and start_dt < cur_dt:
        raise ValueError(f"Start time is in past (it is currently {format_date(cur_dt.isoformat())}).")
    elif end_dt <= start_dt:
        raise ValueError("End time is not after start time.")
    elif end_dt < cur_dt:
        raise ValueError(f"End time is in past (it is currently {format_date(cur_dt.isoformat())}).")

    return start_dt.isoformat(), end_dt.isoformat()


def format_date(s):
    """Return the given datetime string (expected to be UTC and as returned by datetime.isoformat()) in a more
    friendly format.
    """
    return f"<t:{parse_datetime_str(s).timestamp()}>"  # Epoch/Unix time


def format_timedelta(td: timedelta):
    """Given a time delta, return a user-friendly string of days + hours if it is > 1 day, hours + mins
    if it is > 1 min, or seconds if it is <= 1 min.
    """
    # Note that timedelta stores the time as days + seconds + microseconds internally
    if td.days >= 1:
        return f"{td.days} days, {td.seconds // 3600} hours"
    elif td.seconds >= 60:
        return f"{td.seconds // 3600} hours, {(td.seconds // 60) % 60} mins"
    else:
        return f"{td.seconds} seconds"


async def wait_until(dt):
    """Helper to async sleep until after the given Datetime."""
    # Sleep and check time twice for safety since I've found mixed answers on the accuracy of sleeping for week+
    for i in range(3):
        cur_dt = datetime.now(timezone.utc)
        remaining_seconds = (dt - cur_dt).total_seconds()

        if remaining_seconds < 0:
            return
        elif i == 1:
            print(f"BG task attempting to sleep until {dt.isoformat()} only slept until {cur_dt.isoformat()}; re-sleeping")
        elif i == 2:
            raise Exception(f"wait_until waited until {cur_dt.isoformat()} instead of {dt.isoformat()}")

        await asyncio.sleep(remaining_seconds + 0.1)  # Extra 10th of a sec to ensure we go past the specified time
