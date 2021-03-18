#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import shutil

import discord
from discord.ext import commands
from dotenv import load_dotenv
import schem
from slugify import slugify

from metric import validate_metric, eval_metric, format_metric, get_metric_and_terms

load_dotenv()
TOKEN = os.getenv('SCHEM_BOT_DISCORD_TOKEN')
GUILD_ID = int(os.getenv('SCHEM_BOT_GUILD_ID'))
ANNOUNCEMENTS_CHANNEL_ID = int(os.getenv('SCHEM_BOT_ANNOUNCEMENTS_CHANNEL_ID'))
CORANAC_SITE = "https://www.coranac.com/spacechem/mission-viewer"

# TODO: Organize things to be able to use the same commands or code to run standalone puzzle challenges unrelated to
#       any tournament, e.g. puzzle-of-the-week/month, behaving like a standalone tournament round

# TODO: ProcessPoolExecutor might be more appropriate but not sure if the overhead for many small submissions is going
#       to add up more than with threads and/or if limitations on number of processes is the bigger factor
thread_pool_executor = ThreadPoolExecutor()  # TODO max_workers=5 or some such ?
# asyncio.get_event_loop().set_default_executor(thread_pool_executor) # need to make sure this is same event loop as bot

bot = commands.Bot(command_prefix='!',
                   description="SpaceChem-simulating bot."
                               + "\nRuns/validates Community-Edition-exported solution files, excluding legacy bugs.")

@bot.event
async def on_ready():
    print(f'{bot.user.name} has connected to Discord!')

@bot.event
async def on_command_error(ctx, error):
    """Default bot command error handler."""
    if isinstance(error, (commands.CommandNotFound, commands.CheckFailure)):
        return  # Avoid logging errors when users put in invalid commands

    print(f"{type(error).__name__}: {error}")
    await ctx.send(str(error))  # Probably bad practice but it makes the commands' code nice...

@bot.command(name='run', aliases=['r', 'score', 'validate', 'check'])
async def run(ctx):
    """Run/validate the attached solution file.
    Must be a Community Edition export.
    """
    assert len(ctx.message.attachments) == 1, "Expected one attached solution file!"
    soln_bytes = await ctx.message.attachments[0].read()

    try:
        soln_str = soln_bytes.decode("utf-8")
    except UnicodeDecodeError as e:
        raise Exception("Attachment must be a plaintext file (containing a Community Edition export).") from e

    level_name, author, expected_score, soln_name = schem.Solution.parse_metadata(soln_str)
    soln_descr = schem.Solution.describe(level_name, author, expected_score, soln_name)
    msg = await ctx.send(f"Running {soln_descr}, this should take < 30s barring an absurd cycle count...")

    # Call the SChem validator in a thread so the bot isn't blocked
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(thread_pool_executor, schem.validate, soln_str)

    await ctx.message.add_reaction('✅')
    await msg.edit(content=f"Successfully validated {soln_descr}")

# TODO @bot.command(name='random-level')

# TODO: Ideally this and tournament_submit get merged
# @bot.command(name='submit')
# async def submit(ctx):
#     # Sneakily react with a green check mark on msgs to the leaderboard-bot?
#     # Auto-fetch pastebin link from youtube video description


def split_by_char_limit(s, limit=1900):
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


class PuzzleSubmissionsLock:
    """Context manager which allows any number of submitters to a puzzle, until lock_and_wait_for_submitters is called,
    (`await puzzle_submission_lock.lock_and_wait_for_submitters()`), at which point new context
    requesters will receive an exception and the caller will wait for current submissions to finish.
    The lock will remain permanently locked once lock_and_wait_for_submitters() has been called (current
    users: puzzle results announcer and puzzle deleter).
    """
    def __init__(self):
        self.num_submitters = 0
        self.is_closed = False
        self.no_submissions_in_progress = asyncio.Event()
        self.no_submissions_in_progress.set()  # Set right away since no submitters to start

    async def lock_and_wait_for_submitters(self):
        """Block new submitters from opening the context and wait for all current submitters to finish.
        May only be called once.
        """
        if self.is_closed:
            raise Exception("This puzzle has already been locked!")

        self.is_closed = True
        await self.no_submissions_in_progress.wait()  # Wait for all current submitters to exit their contexts

    def unlock(self):
        """Re-allow the lock to be claimed."""
        self.is_closed = False

    def __enter__(self):
        """Raise an exception if lock_and_wait_for_submitters() has already been called, else register as a submitter
        and enter the context.
        """
        # Note that submissions check more precisely against puzzle end time, as long as we make it wait ~5 seconds to
        # ensure the async loop has time to call every pre-deadline submit, the results announcer should never be able
        # to block submitters (since they should check message time and grab the lock immediately)
        if self.is_closed:
            raise Exception("This puzzle has been locked for updates, if the round is still open please try again in"
                            " a few minutes.")

        self.num_submitters += 1
        self.no_submissions_in_progress.clear()  # Anyone waiting for all submissions to complete will now be blocked

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Unregister as a submitter and indicate to any listeners if there are now no remaining submitters."""
        self.num_submitters -= 1
        if self.num_submitters == 0:
            # Indicate that any wait_and_block caller may open the context
            self.no_submissions_in_progress.set()


# Used in a decorator for checking if a user has tournament-hosting permissions.
# Unfortunately can't be part of the Tournament cog since command.check doesn't pass self/cls
def is_tournament_host(ctx):
    """Check whether the given user has tournament-hosting permissions."""
    hosts_json_file = Tournament.TOURNAMENTS_DIR / 'hosts.json'
    if not hosts_json_file.exists():
        return False

    with open(hosts_json_file, encoding='utf-8') as f:
        return str(ctx.message.author) in json.load(f)['hosts']


class Tournament(commands.Cog):  # name="Help text name?"
    """Tournament Commands"""

    # Tournaments structure:
    # tournaments/
    #     hosts.json -> list of discord users with admin access to tournament commands
    #     active_tournament.txt -> "slugified_tournament_name_1"
    #     slugified_tournament_name_1/
    #         tournament_metadata.json -> name, host, etc, + round dirs / metadata
    #         participants.json        -> discord_tag: name (as it will appear in solution exports)
    #         standings.json           -> 'rounds': {puzzle_name: {player: score}}, 'total': {player: score}
    #         bonus1_puzzleA/
    #         round1_puzzleB/
    #             puzzleB.puzzle
    #             solutions.txt
    #             solutions_fun.txt
    #         round2_puzzleC/
    #         ...
    #     slugified_tournament_name_2/
    #     ...

    # Decorator for host-only commands
    is_host = commands.check(is_tournament_host)

    TOURNAMENTS_DIR = Path(__file__).parent / 'tournaments'  # Left relative so that filesystem paths can't leak into bot msgs
    ACTIVE_TOURNAMENT_FILE = TOURNAMENTS_DIR / 'active_tournament.txt'

    # Lock to ensure async calls don't overwrite each other
    # Should be used by any call that is writing to tournament_metadata.json. It is also assumed that no writer
    # calls await after having left the metadata in bad state. Given this, readers need not acquire the lock.
    tournament_metadata_write_lock = asyncio.Lock()

    # TODO: Keep metadata and/or other tournament files in memory to avoid excess file-reads (but still write to files)
    def __init__(self, bot):
        self.bot = bot

        # Bot announcement tasks, stored so we can update them if relevant metadata changes
        self.tournament_start_task = None
        self.round_start_tasks = {}
        self.puzzle_submission_locks = {}
        self.round_results_tasks = {}
        self.tournament_results_task = None

        # Start relevant announcement tasks. These will schedule themselves based on current tournament metadata
        if self.ACTIVE_TOURNAMENT_FILE.exists():
            _, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            # Schedule the next announcement task(s).
            if 'start_post' not in tournament_metadata:
                self.tournament_start_task = self.bot.loop.create_task(self.announce_tournament_start(tournament_metadata))
            else:
                # Only scheduling these after the tournament start announcement ensures that any puzzle starting at the
                # same time as the tournament (e.g. test puzzle) will not be announced before the tournament itself
                for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                    if 'start_post' not in round_metadata:
                        # If the puzzle has not been announced yet, schedule the announcement task
                        self.round_start_tasks[puzzle_name] = self.bot.loop.create_task(self.announce_round_start(puzzle_name, round_metadata))
                    elif 'end_post' not in round_metadata:
                        # If the puzzle has opened but not ended, add a submissions lock and results announcement task
                        self.puzzle_submission_locks[puzzle_name] = PuzzleSubmissionsLock()
                        self.round_results_tasks[puzzle_name] = self.bot.loop.create_task(self.announce_round_results(puzzle_name, round_metadata))

                if 'end_post' not in tournament_metadata:
                    self.tournament_results_task = self.bot.loop.create_task(self.announce_tournament_results(tournament_metadata))

    def get_active_tournament_dir_and_metadata(self, is_host=False):
        """Helper to fetch the active tournament directory and metadata. Raise error if there is no active tournament
        or if is_host not provided and the tournament hasn't been announced.
        """
        no_tournament_exc = FileNotFoundError("No active tournament!")
        if not self.ACTIVE_TOURNAMENT_FILE.exists():
            raise no_tournament_exc

        with open(self.ACTIVE_TOURNAMENT_FILE, 'r', encoding='utf-8') as f:
            tournament_dir = self.TOURNAMENTS_DIR / f.read().strip()

        with open(tournament_dir / 'tournament_metadata.json', 'r', encoding='utf-8') as f:
            tournament_metadata = json.load(f)

        if 'start_post' not in tournament_metadata and not is_host:
            raise no_tournament_exc

        return tournament_dir, tournament_metadata

    def get_player_name(self, tournament_dir, discord_user: discord.User, missing_ok=True):
        """Given a discord user, get their tournament nickname as set by their first submission."""
        discord_tag = str(discord_user)
        with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
            participants = json.load(f)

        if discord_tag not in participants:
            if missing_ok:
                return None
            else:
                raise Exception("You have no current tournament submissions.")

        return participants[discord_tag]

    def get_level(self, round_dir):
        """Given a round directory, return an schem.Level object based on its .puzzle file."""
        puzzle_file = next(round_dir.glob('*.puzzle'), None)
        if puzzle_file is None:
            print(f"Error: {round_dir} puzzle file not found!")
            raise FileNotFoundError("Round puzzle file not found; I seem to be experiencing an error.")

        with open(puzzle_file, 'r', encoding='utf-8') as f:
            level_code = f.read().strip()

        return schem.Level(level_code)

    def parse_datetime_str(self, s):
        """Parse and check validity of given ISO date string then return as a UTC Datetime (converting as needed)."""
        dt = datetime.fromisoformat(s.rstrip('Z'))  # For some reason isoformat doesn't like Z (Zulu time) suffix

        # If timezone unspecified, assume UTC, else convert to UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        return dt

    def process_tournament_dates(self, start, end, check_start_in_future=True):
        """Helper for validating and reformatting tournament start/end date args (e.g. tournament or round start & end).
        When updating an existing tournament, check for start date being in future should be ignored.
        """
        start_dt = self.parse_datetime_str(start)
        end_dt = self.parse_datetime_str(end)
        cur_dt = datetime.now(timezone.utc)

        if check_start_in_future and start_dt < cur_dt:
            raise ValueError("Start time is in past.")
        elif end_dt <= start_dt:
            raise ValueError("End time is not after start time.")
        elif end_dt < cur_dt:
            raise ValueError("End time is in past.")

        return start_dt.isoformat(), end_dt.isoformat()

    @classmethod
    def format_tournament_datetime(cls, s):
        """Return the given datetime string (expected to be UTC and as returned by datetime.isoformat()) in a more
        friendly format.
        """
        return ' '.join(s[:-9].split('T')) + ' UTC'  # Remove T and the seconds field, and replace '+00:00' with ' UTC'

    def hosts(self):
        """Return a set of the users with tournament-hosting permissions."""
        hosts_json_file = self.TOURNAMENTS_DIR / 'hosts.json'
        if not hosts_json_file.exists():
            return set()

        with open(hosts_json_file, encoding='utf-8') as f:
            return set(json.load(f)['hosts'])

    # Note: Command docstrings should be limited to ~80 char lines to avoid ugly wraps in any reasonably-sized window
    @commands.command(name='tournament-hosts', aliases=['th'])
    @is_host
    async def hosts_cmd(self, ctx):
        """List all tournament hosts."""
        await ctx.send(f"The following users have tournament-hosting permissions: {', '.join(self.hosts())}")

    @commands.command(name='tournament-host-add', aliases=['add-tournament-host'])
    @commands.is_owner()
    #@commands.dm_only()
    async def add_tournament_host(self, ctx, user: discord.User):
        """Give someone tournament-hosting permissions."""
        discord_tag = str(user)  # e.g. <username>#1234. Guaranteed to be unique

        self.TOURNAMENTS_DIR.mkdir(exist_ok=True)

        hosts = self.hosts()
        if discord_tag in hosts:
            raise ValueError("Given user is already a tournament host")
        hosts.add(discord_tag)

        with open(self.TOURNAMENTS_DIR / 'hosts.json', 'w', encoding='utf-8') as f:
            json.dump({'hosts': list(hosts)}, f)

        await ctx.send(f"{discord_tag} added to tournament hosts.")

    @commands.command(name='tournament-host-remove', aliases=['remove-tournament-host'])
    @commands.is_owner()
    #@commands.dm_only()
    async def remove_tournament_host(self, ctx, user: discord.User):
        """Remove someone's tournament-hosting permissions."""
        discord_tag = str(user)  # e.g. <username>#1234. Guaranteed to be unique

        hosts = self.hosts()
        if discord_tag not in hosts:
            raise ValueError("Given user is not a tournament host")
        hosts.remove(discord_tag)

        with open(self.TOURNAMENTS_DIR / 'hosts.json', 'w', encoding='utf-8') as f:
            json.dump({'hosts': list(hosts)}, f, ensure_ascii=False, indent=4)

        await ctx.send(f"{discord_tag} removed from tournament hosts.")

    @commands.command(name='tournament-create', aliases=['tc'])
    @is_host
    #@commands.dm_only()
    async def tournament_create(self, ctx, name, start, end):
        """Create a tournament.

        There may only be one tournament pending/active at a time.

        name: The tournament's official name, e.g. "2021 SpaceChem Tournament"
        start: The datetime on which the bot will announce the tournament publicly and
               after which puzzle rounds may start. ISO format, default UTC.
               E.g. the following are all equivalent: 2000-01-31, "2000-01-31 00:00",
                    2000-01-30T19:00:00-05:00
        end: The datetime on which the bot will announce the tournament results,
             after closing and tallying the results of any still-open puzzles.
             Same format as `start`.
        """
        tournament_dir_name = slugify(name)  # Convert to a valid directory name
        assert tournament_dir_name, f"Invalid tournament name {name}"

        start, end = self.process_tournament_dates(start, end)

        self.TOURNAMENTS_DIR.mkdir(exist_ok=True)

        async with self.tournament_metadata_write_lock:
            if self.ACTIVE_TOURNAMENT_FILE.exists():
                raise FileExistsError("There is already an active or upcoming tournament.")

            tournament_dir = self.TOURNAMENTS_DIR / tournament_dir_name
            tournament_dir.mkdir(exist_ok=False)

            with open(self.ACTIVE_TOURNAMENT_FILE, 'w', encoding='utf-8') as f:
                f.write(tournament_dir_name)

            # Initialize tournament metadata, participants (discord_id: soln_author_name), and standings files
            tournament_metadata = {'name': name, 'host': ctx.message.author.name, 'start': start, 'end': end, 'rounds': {}}
            with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            with open(tournament_dir / 'participants.json', 'w', encoding='utf-8') as f:
                json.dump({}, f, ensure_ascii=False, indent=4)

            with open(tournament_dir / 'standings.json', 'w', encoding='utf-8') as f:
                json.dump({'rounds':{}, 'total': {}}, f, ensure_ascii=False, indent=4)

            # Schedule the tournament announcement
            self.tournament_start_task = self.bot.loop.create_task(self.announce_tournament_start(tournament_metadata))

        reply = f"Successfully created {repr(name)}"
        reply += f", Start: {self.format_tournament_datetime(tournament_metadata['start'])}"
        reply += f", End: {self.format_tournament_datetime(tournament_metadata['end'])}"
        await ctx.send(reply)

    @commands.command(name='tournament-update', aliases=['tu'])
    @is_host
    #@commands.dm_only()
    async def tournament_update(self, ctx, new_name, new_start, new_end):
        """Update the current/pending tournament.

        new_name: The tournament's official name, e.g. "2021 SpaceChem Tournament"
        new_start: The datetime on which the bot will announce the tournament and
                   after which puzzle rounds may start. ISO format, default UTC.
                   E.g. the following are all equivalent: 2000-01-31, "2000-01-31 00:00",
                        2000-01-30T19:00:00-05:00
        new_end: The datetime on which the bot will announce the tournament results,
                 after closing and tallying the results of any still-open puzzles.
                 Same format as `new_start`.
        """
        updated_fields = []

        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            # Process dates while ignoring start being in the past, for the case where we didn't modify it
            new_start, new_end = self.process_tournament_dates(new_start, new_end, check_start_in_future=False)

            if new_start != tournament_metadata['start']:
                assert 'start_post' not in tournament_metadata, "Cannot update start date on already-announced tournament"

                # If we're modifying the start date, we do have to make sure it's in the future
                if new_start < datetime.now(timezone.utc).isoformat():
                    raise ValueError("New start date is in past")

                # Check that this doesn't violate any puzzle start dates
                for round_metadata in tournament_metadata['rounds'].values():
                    if new_start > round_metadata['start']:  # Safe since we convert everything to ISO and UTC
                        raise ValueError(f"New start date is after start of {repr(round_metadata['round_name'])}")

                tournament_metadata['start'] = new_start
                updated_fields.append('start date')

            modified_round_ends = []
            if new_end != tournament_metadata['end']:
                # Change the end date of any puzzles that ended at the same time as the tournament
                # For all other puzzles, check their end dates weren't violated
                for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                    # Again all below date comparisons safe since everything is ISO and UTC format
                    if round_metadata['end'] == tournament_metadata['end']:
                        # Check round start isn't violated before we modify round end
                        if round_metadata['start'] >= new_end:
                            raise ValueError(f"New end date is before start of {repr(round_metadata['round_name'])}")

                        round_metadata['end'] = new_end

                        # Update the results announcement task if it exists
                        if puzzle_name in self.round_results_tasks:
                            self.round_results_tasks[puzzle_name].cancel()
                            self.tournament_results_task = self.bot.loop.create_task(self.announce_round_results(puzzle_name, round_metadata))

                        modified_round_ends.append(round_metadata['round_name'])
                    elif new_end < round_metadata['end']:
                        raise ValueError(f"New end date is before end of {repr(round_metadata['round_name'])}")

                tournament_metadata['end'] = new_end
                updated_fields.append('end date')

            # Update name last so the directory rename won't occur if other args were invalid
            if new_name != tournament_metadata['name']:
                new_tournament_dir_name = slugify(new_name)
                assert new_tournament_dir_name, f"Invalid tournament name {new_name}"

                tournament_dir.rename(self.TOURNAMENTS_DIR / new_tournament_dir_name)
                tournament_dir = self.TOURNAMENTS_DIR / new_tournament_dir_name

                with open(self.ACTIVE_TOURNAMENT_FILE, 'w', encoding='utf-8') as f:
                    f.write(new_tournament_dir_name)

                tournament_metadata['name'] = new_name
                updated_fields.append('name')

            if not updated_fields:
                raise ValueError("All fields match existing tournament fields.")

            with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            # If the update was successful and changed a date(s), cancel and replace the relevant BG announcement task
            if 'start date' in updated_fields:
                self.tournament_start_task.cancel()
                self.tournament_start_task = self.bot.loop.create_task(self.announce_tournament_start(tournament_metadata))
            elif 'end date' in updated_fields and 'start_post' in tournament_metadata:
                self.tournament_results_task.cancel()
                self.tournament_results_task = self.bot.loop.create_task(self.announce_tournament_results(tournament_metadata))

        reply = f"Successfully updated tournament {', '.join(updated_fields)}."
        if modified_round_ends:
            reply += f"\nEnd date of round(s) `{'`, `'.join(modified_round_ends)}` updated to match new end date."

        await ctx.send(reply)

    # TODO: @commands.command(name='tournament-delete')  # Is this existing too dangerous?
    #                                                    # In any case tournament-update should be sufficient for now

    def get_puzzle_name(self, tournament_metadata, round_or_puzzle_name, is_host=False, missing_ok=True):
        """Given a string, return the puzzle name for any puzzle/round matching it case-insensitively.

        is_host: Hide future puzzles if not True; default False.
        missing_ok: If True, return None if the puzzle is missing; else raise exception. Default True.
        """
        lower_name = round_or_puzzle_name.lower()
        for cur_puzzle_name, round_metadata in tournament_metadata['rounds'].items():
            if ((is_host or 'start_post' in round_metadata)
                    and lower_name in (cur_puzzle_name.lower(), round_metadata['round_name'].lower())):
                return cur_puzzle_name

        if missing_ok:
            return None

        raise FileNotFoundError(f"No known puzzle/round ~= `{round_or_puzzle_name}`")

    async def read_puzzle_attachment(self, discord_file):
        if not discord_file.filename.endswith('.puzzle'):
            # TODO: Could fall back to slugify(level.name) or slugify(round_name) for the .puzzle file name if the
            #       extension doesn't match
            raise ValueError("Attached file should use the extension .puzzle")

        level_bytes = await discord_file.read()
        try:
            return level_bytes.decode("utf-8")
        except UnicodeDecodeError as e:
            raise Exception("Attachment must be a plaintext file (containing a level export code).") from e

    # TODO: Puzzle flavour text
    @commands.command(name='tournament-puzzle-add', aliases=['tpa', 'tournament-add-puzzle', 'tap'])
    @is_host
    #@commands.dm_only()
    async def tournament_add_puzzle(self, ctx, round_name, metric, points: float, start, end=None):
        """Add a puzzle to the tournament.

        round_name: e.g. "Round 1" or "Bonus 1".
        metric: The equation a player should minimize.
                A player's final score for the round will be the top metric
                score divided by this metric score.
                Allowed terms: <Any real number>, cycles, reactors, symbols,
                               waldopath, waldos, bonders, arrows, flip_flops,
                               sensors, syncs.
                Allowed operators/fns: ^ (or **), /, *, +, -, max(), min(),
                                       log() (base 10)
                Parsed with standard operator precedence (BEDMAS).
                E.g.: "cycles + 0.1 * symbols + bonders^2"
        points: # of points that the first place player will receive.
                Other players will get points proportional to this based
                on their relative metric score.
        start: The datetime on which the puzzle will be announced and submissions opened.
               ISO format, default UTC.
               E.g. the following are all equivalent: 2000-01-31, "2000-01-31 00:00",
                    2000-01-30T19:00:00-05:00
        end: The datetime on which submissions will close and the results will be
             announced. Same format as `start`.
             If excluded, puzzle is open until the tournament is ended (e.g. the
             2019 tournament's 'Additional' puzzles).
        """
        # Check attached puzzle
        assert len(ctx.message.attachments) == 1, "Expected one attached puzzle file!"
        puzzle_file = ctx.message.attachments[0]
        level_code = await self.read_puzzle_attachment(puzzle_file)
        level = schem.Level(level_code)

        validate_metric(metric)

        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            # Check if either of puzzle/round name are too similar to existing names of either type (since e.g.
            # tournament-info searches case-insensitively for either name)
            for name in (level.name, round_name):
                if self.get_puzzle_name(tournament_metadata, name, is_host=True, missing_ok=True) is not None:
                    raise ValueError(f"Puzzle/round with name ~= `{name}` already exists in the current tournament")
            round_dir_name = f'{slugify(round_name)}_{slugify(level.name)}'  # Human-friendly directory name

            # Validate start/end datetimes
            if end is None:
                end = tournament_metadata['end']

            start, end = self.process_tournament_dates(start, end)  # Format and basic temporal sanity checks

            # Also check against the tournament start/end dates
            # String comparisons are safe here because all datetimes have been converted to ISO + UTC format
            if start < tournament_metadata['start']:
                raise ValueError(f"Round start time is before tournament start ({tournament_metadata['start']}).")
            elif end > tournament_metadata['end']:
                raise ValueError(f"Round end time is after tournament end ({tournament_metadata['end']}).")

            tournament_metadata['rounds'][level.name] = {'dir': round_dir_name,
                                                         'round_name': round_name,
                                                         'metric': metric,
                                                         'points': points,
                                                         'start': start,
                                                         'end': end}

            # Re-sort rounds by start date
            tournament_metadata['rounds'] = dict(sorted(tournament_metadata['rounds'].items(),
                                                        key=lambda x: x[1]['start']))

            # Set up the round directory
            round_dir = tournament_dir / round_dir_name
            round_dir.mkdir(exist_ok=False)
            await puzzle_file.save(round_dir / puzzle_file.filename)
            (round_dir / 'solutions.txt').touch()
            (round_dir / 'solutions_fun.txt').touch()

            with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            # Schedule the round announcement (if the start announcement task has already run and won't do it for us)
            if 'start_post' in tournament_metadata:
                self.round_start_tasks[level.name] = \
                    self.bot.loop.create_task(self.announce_round_start(level.name, tournament_metadata['rounds'][level.name]))

            # TODO: Track the history of each player's scores over time and do cool graphs of everyone's metrics going
            #       down as the deadline approaches!
            #       Can do like the average curve of everyone's scores over time and see how that curve varies by level
            #       Probably don't store every solution permanently to avoid the tournament.zip getting bloated but can
            #       at least keep the scores from replaced solutions.

            # TODO 2: Pareto frontier using the full submission history!

        await ctx.send(f"Successfully added {round_name} {level.name} to {tournament_metadata['name']}")

    async def wait_for_confirmation(self, ctx, confirm_msg, confirm_react='✅', cancel_react='❌', timeout_seconds=30):
        """Wait for a reaction to the given message confirming an operation (by the user who created the passed
        context), returning True if they confirm and False otherwise. If the message is cancelled or the given timeout
        is reached, also send a message in the given context indicating the operation was cancelled.
        """
        def check(reaction_event):
            return (reaction_event.message_id == confirm_msg.id
                    and reaction_event.user_id == ctx.message.author.id
                    and str(reaction_event.emoji) in (confirm_react, cancel_react))

        try:
            # reaction_add doesn't work in DMs without the `members` intent given to the Bot constructor, which we don't
            # really need (see https://discordpy.readthedocs.io/en/latest/api.html#discord.on_reaction_add)
            reaction_event = await self.bot.wait_for('raw_reaction_add', timeout=timeout_seconds, check=check)

            if str(reaction_event.emoji) == confirm_react:
                return True
            else:
                await ctx.send('Operation cancelled!')
                return False
        except asyncio.TimeoutError:
            await ctx.send('Operation cancelled!')
            return False

    @commands.command(name='tournament-puzzle-update', aliases=['tournament-puzzle-edit', 'tournament-update-puzzle',
                                                                'tournament-edit-puzzle'])
    @is_host
    #@commands.dm_only()
    async def update_puzzle(self, ctx, round_or_puzzle_name, *update_fields):  # TODO: public_explanation_blurb
        """Update the specified puzzle.

        If the puzzle is already open, a post announcing the updated fields will
        be made and the original announcement post edited.

        If the puzzle file is also updated, the following will also occur:
            - Player solutions will be re-validated, and any invalidated
              solutions will be removed and their authors DM'd to inform
              them of this.
            - As attachments cannot be edited/deleted, instead of editing
              the original announcement post, a new announcement post will
              be made (after the change summary post), and linked to from
              the old announcement post.

        round_or_puzzle_name: (Case-insensitive) Round or puzzle to update.
                              May not be a closed puzzle.
        update_fields: Fields to update, specified like:
                       field1=value "field2=value with spaces"
                       Valid fields (same as tournament-add-puzzle):
                           round_name, metric, points, start, end
                       Double-quotes within a field's value should be avoided
                       as they will interfere with arg-parsing.
                       Unspecified fields will not be modified.
                       A new puzzle file may also be attached.
        E.g.: !tournament-puzzle-update "Round 3" "round_name=Round 3.5" end=2000-01-31T19:17-05:00
        """
        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)
            puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name, is_host=True, missing_ok=False)
            round_metadata = tournament_metadata['rounds'][puzzle_name]

            if 'end_post' in round_metadata:
                raise Exception("Cannot edit closed puzzle.")

            parser = argparse.ArgumentParser(exit_on_error=False)
            parser.add_argument('--round_name')
            parser.add_argument('--metric')
            parser.add_argument('--points', type=float)
            parser.add_argument('--start', '--start_date')
            parser.add_argument('--end', '--end_date')

            # Heavy-handed SystemExit catch because even with exit_on_error, unknown args can cause an exit:
            # https://bugs.python.org/issue41255
            try:
                args = parser.parse_args(f'--{s}' for s in update_fields)
            except SystemExit:
                raise Exception("Unrecognized arguments included, double-check `!help tournament-puzzle-update`")

            args_dict = vars(args)
            updated_fields = set(k for k, v in args_dict.items() if v)

            # Check that only changed fields were specified
            for k, v in args_dict.items():
                if v and v == round_metadata[k]:
                    raise ValueError(f"{k} was already `{v}`, did you mean to update it?")

            assert not args.start or 'start_post' not in round_metadata, "Cannot update start date; puzzle is already open!"

            # Make sure new round name doesn't conflict with any existing ones
            old_round_name = round_metadata['round_name']
            if (args.round_name
                    and self.get_puzzle_name(tournament_metadata, args.round_name,
                                             is_host=True, missing_ok=True) is not None):
                raise ValueError(f"Puzzle/round with name ~= `{args.round_name}` already exists in the current tournament")

            # Reformat and do basic checks on the date args
            if args.start or args.end:
                start, end = self.process_tournament_dates(args.start if args.start else round_metadata['start'],
                                                           args.end if args.end else round_metadata['end'],
                                                           check_start_in_future=False)
                if args.start:
                    args.start = start

                if args.end:
                    args.end = end

            if args.metric:
                validate_metric(args.metric)

            # Prepare a text post summarizing the changed fields
            # TODO: @tournament or some such
            summary_text = (f"**The tournament host has updated {round_metadata['round_name']}, {puzzle_name}**"
                            + "\nChanges:")

            # Update round metadata and summary text
            for k, v in args_dict.items():
                if v:
                    summary_text += f"\n  • {k}: `{round_metadata[k]}` -> `{v}`"
                    round_metadata[k] = v

            try:
                if ctx.message.attachments:
                    assert len(ctx.message.attachments) == 1, "Expected at most a single attached puzzle file!"
                    new_puzzle_file = ctx.message.attachments[0]
                    new_level_code = (await self.read_puzzle_attachment(new_puzzle_file)).strip()
                    level = schem.Level(new_level_code)

                    # Make sure the new puzzle name doesn't conflict with any other rounds/puzzles
                    if not (self.get_puzzle_name(tournament_metadata, level.name,
                                                 is_host=True, missing_ok=True) in (None, puzzle_name)):
                        raise ValueError(f"Puzzle/round with name ~= `{level.name}` already exists in the current tournament")

                    # Update the puzzle name in metadata. We'll leave the directory name unchanged until after confirmation
                    new_puzzle_name = level.name
                    del tournament_metadata['rounds'][puzzle_name]
                    tournament_metadata['rounds'][new_puzzle_name] = round_metadata

                    updated_fields.add("puzzle file")
                    summary_text += ("\n  • Puzzle file changed."
                                     "\n    Any players whose solutions were invalidated by this change have been DM'd.")

                    # Double-check that the puzzle code actually changed
                    round_dir = tournament_dir / round_metadata['dir']
                    old_puzzle_file = next(round_dir.glob('*.puzzle'), None)
                    assert old_puzzle_file is not None, "Internal Error: puzzle file for specified round is missing"
                    with open(old_puzzle_file, 'r', encoding='utf-8') as f:
                        old_level_code = f.read().strip()
                    old_level = schem.Level(old_level_code)
                    assert level != old_level, "Attached puzzle has not changed, did you mean to update it?"

                    # First check whether this puzzle file will invalidate any solutions
                    if 'start_post' in round_metadata:
                        # Wait for current submitters to finish adding solutions then temporarily block new submitters
                        await self.puzzle_submission_locks[puzzle_name].lock_and_wait_for_submitters()

                        msg = await ctx.send("Re-validating player submissions, this may take a few minutes...")
                        loop = asyncio.get_event_loop()
                        invalid_soln_authors = set()
                        valid_soln_strs = {}

                        for solns_file_name in ('solutions.txt', 'fun_solutions.txt'):
                            solns_file = round_dir / solns_file_name
                            if not solns_file.is_file():
                                continue

                            valid_soln_strs[solns_file_name] = []
                            with open(solns_file, 'r', encoding='utf-8') as f:
                                solns_str = f.read()

                            for soln_str in schem.Solution.split_solutions(solns_str):
                                _, author_name, _, _ = schem.Solution.parse_metadata(soln_str)

                                # Call the SChem validator in a thread so the bot isn't blocked
                                # TODO: If/when 'outputs' is a metric term, will need to update this similarly to submit to
                                #       allow partial solutions in its presence
                                try:
                                    solution = schem.Solution(level, soln_str)
                                    await loop.run_in_executor(thread_pool_executor, solution.validate)
                                except Exception:
                                    invalid_soln_authors.add(author_name)
                                    continue

                                # TODO: If the solution string was still valid and the level name changed, update the
                                #       solution's level name

                                valid_soln_strs[solns_file_name].append(soln_str)

                        # Prepare a new announcement post and the puzzle file to attach
                        # Pass the attached puzzle file instead of using the round's
                        # TODO: This is pr hacky, maybe should separate the attachment generation from round_announcement
                        new_announcement_embed, new_announcement_attachment = \
                            self.round_announcement(tournament_dir, tournament_metadata, new_puzzle_name,
                                                    level_code=new_level_code, attachment=(await new_puzzle_file.to_file()))
                else:
                    assert updated_fields, "Missing fields to update or puzzle file attachment!"
                    new_puzzle_name = puzzle_name

                    # If the round is open but we don't need to change the puzzle file, prepare an edit to the original
                    # announcements post
                    if 'start_post' in round_metadata:
                        # Update the announcement using the modified tournament metadata
                        edited_announcement_embed = self.round_announcement(tournament_dir, tournament_metadata, new_puzzle_name)[0]

                if 'start_post' in round_metadata:
                    msg_id = round_metadata['start_post'].strip('/').split('/')[-1]
                    channel = self.bot.get_channel(ANNOUNCEMENTS_CHANNEL_ID)
                    og_announcement = await channel.fetch_message(msg_id)

                    # Preview the changes-summary post, the new or edited announcement post, and the names of all players
                    # whose solutions were invalidated, and ask for TO confirmation
                    if ctx.message.attachments:
                        # Edit the 'running solutions...' message
                        await msg.edit(content="The specified puzzle has already been opened so the following public"
                                       " announcement posts will be made:\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                        await ctx.send(summary_text)
                        await ctx.send(embed=new_announcement_embed, file=new_announcement_attachment)
                        edit_note_text = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" \
                                         + "The original announcement post will be edited to include a link to the above post."
                        if invalid_soln_authors:
                            edit_note_text += "\n**Additionally, this change to the puzzle invalidated the following players'" \
                                              + f" solutions**: {', '.join(invalid_soln_authors)}" \
                                              + "\nThese players will be DM'd to inform them of their removed solution(s)."
                        else:
                            edit_note_text += "\n**No player solutions were invalidated by this puzzle file change.**"
                        await ctx.send(edit_note_text)
                    else:
                        await ctx.send("The specified puzzle has already been opened so the following public announcement"
                                       " post will be made:\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                        await ctx.send(summary_text)
                        await ctx.send("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                                       "\nand the original announcement post will be edited to read:")
                        await ctx.send(embed=edited_announcement_embed,
                                       file=(await og_announcement.attachments[0].to_file()))
                        await ctx.send("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

                    # Ask the TO for confirmation before making any changes
                    confirm_msg = await ctx.send("Are you sure you wish to continue?"
                                                 " React with ✅ within 30 seconds to proceed, ❌ to cancel all changes.")
                    if not await self.wait_for_confirmation(ctx, confirm_msg):
                        return

                    # If the round directory will be renamed, wait for any submitters to finish modifying its contents
                    # if we haven't already
                    if args.round_name and not ctx.message.attachments:
                        await self.puzzle_submission_locks[puzzle_name].lock_and_wait_for_submitters()

                    if ctx.message.attachments:
                        # Save the new puzzle file and update the tournament metadata's puzzle name as needed
                        old_puzzle_file.unlink()
                        await new_puzzle_file.save(round_dir / new_puzzle_file.filename)
                        del tournament_metadata['rounds'][puzzle_name]
                        tournament_metadata['rounds'][new_puzzle_name] = round_metadata

                        # Update solutions.txt and fun_solutions.txt
                        for solns_file_name, cur_soln_strs in valid_soln_strs.items():
                            with open(round_dir / solns_file_name, 'w', encoding='utf-8') as f:
                                f.write('\n'.join(cur_soln_strs))

                    # Make the changes-summary post and edit the original announcement post or make the new post if
                    # the puzzle file changed
                    msg_id = round_metadata['start_post'].strip('/').split('/')[-1]
                    og_announcement = await channel.fetch_message(msg_id)
                    if ctx.message.attachments:
                        # Make the changes-summary post
                        await channel.send(summary_text + "\n\nNew announcement post:")

                        msg = await channel.send(embed=new_announcement_embed, file=(await new_puzzle_file.to_file()))
                        await og_announcement.edit(
                            content=f"**EDIT: This puzzle has been updated, see the new announcement post here**: {msg.jump_url}")
                        round_metadata['start_post'] = msg.jump_url
                    else:
                        await og_announcement.edit(embed=edited_announcement_embed)

                        await channel.send(summary_text
                                           + f"\n\nThe announcement post has been edited: {og_announcement.jump_url}")

                    # Create a new (open) submissions lock
                    del self.puzzle_submission_locks[puzzle_name]
                    self.puzzle_submission_locks[new_puzzle_name] = PuzzleSubmissionsLock()
                elif ctx.message.attachments:
                    # Save the new puzzle file and update the tournament metadata's puzzle name as needed
                    old_puzzle_file.unlink()
                    await new_puzzle_file.save(round_dir / new_puzzle_file.filename)
                    del tournament_metadata['rounds'][puzzle_name]
                    tournament_metadata['rounds'][new_puzzle_name] = round_metadata
            finally:
                # Make sure we re-unlock the puzzle even if the process was rejected or was cancelled
                # Note that this will have no effect if the lock was already restored or has changed names
                if puzzle_name in self.puzzle_submission_locks:
                    self.puzzle_submission_locks[puzzle_name].unlock()

            # Update and move the round directory
            old_round_dir = tournament_dir / round_metadata['dir']
            round_metadata['dir'] = f"{slugify(round_metadata['round_name'])}_{slugify(new_puzzle_name)}"
            shutil.move(old_round_dir, tournament_dir / round_metadata['dir'])

            # Update the tournament metadata
            with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            # TODO: tasks and locks need to change their puzzle name
            # Replace the relevant announcement task if a date changed
            if 'start' in updated_fields and puzzle_name in self.round_start_tasks:
                self.round_start_tasks[puzzle_name].cancel()
                del self.round_start_tasks[puzzle_name]
                self.round_start_tasks[new_puzzle_name] = self.bot.loop.create_task(self.announce_round_start(puzzle_name, round_metadata))

            if 'end' in updated_fields and puzzle_name in self.round_results_tasks:
                self.round_results_tasks[puzzle_name].cancel()
                del self.round_results_tasks[puzzle_name]
                self.round_results_tasks[new_puzzle_name] = self.bot.loop.create_task(self.announce_round_results(puzzle_name, round_metadata))

            # DM any players whose solutions were invalidated
            if 'start_post' in round_metadata and ctx.message.attachments and invalid_soln_authors:
                with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
                    participants = json.load(f)

                for member in self.bot.get_guild(GUILD_ID).members:
                    discord_tag = str(member)
                    if discord_tag in participants and participants[discord_tag] in invalid_soln_authors:
                        await self.bot.send_message(
                            member,
                            f"{old_round_name}, {puzzle_name} has been updated and one or more of your submissions"
                            " were invalidated by the change! Please check"
                            f' `!tournament-list-submissions "{round_metadata["round_name"]}"` and update/re-submit'
                            " any missing solutions as needed.")

        await ctx.send(f"Updated {', '.join(updated_fields)} for {round_metadata['round_name']}, {puzzle_name}")

    @commands.command(name='tournament-puzzle-delete', aliases=['tournament-delete-puzzle'])
    @is_host
    #@commands.dm_only()
    async def delete_puzzle(self, ctx, *, round_or_puzzle_name):
        """Delete a round/puzzle.

        round_or_puzzle_name: (Case-insensitive) If provided, show only your submissions
                              to the specified round/puzzle. May be a past puzzle.
        """
        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            # Convert to puzzle name
            puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name,
                                               is_host=True, missing_ok=False)

            round_metadata = tournament_metadata['rounds'][puzzle_name]
            round_dir = tournament_dir / round_metadata['dir']
            round_name = round_metadata['round_name']

            msg = None

            # Ask for confirmation before deleting if the round start date has passed
            if datetime.now(timezone.utc).isoformat() > round_metadata['start']:
                timeout_seconds = 30
                warn_msg = await ctx.send(
                    f"Warning: This round's start date ({self.format_tournament_datetime(round_metadata['start'])}) has"
                    + " already passed and deleting it will delete any player solutions. Are you sure you wish to continue?"
                    + f"\nReact to this message with ✅ within {timeout_seconds} seconds to delete anyway, ❌ to cancel.")

                if not await self.wait_for_confirmation(ctx, warn_msg):
                    return

                if puzzle_name in self.puzzle_submission_locks:
                    msg = await ctx.send("Waiting for any current submitters...")
                    await self.puzzle_submission_locks[puzzle_name].lock_and_wait_for_submitters()
                    del self.puzzle_submission_locks[puzzle_name]

            # Subtract this puzzle from the tournament standings if its results have already been tallied
            if 'end_post' in round_metadata:
                with open(tournament_dir / 'standings.json', 'r', encoding='utf-8') as f:
                    standings = json.load(f)

                for player, points in standings['rounds'][puzzle_name].items():
                    if player in standings['total']:  # Check needed due to 0-scores being excluded from total
                        standings['total'][player] -= points
                        if standings['total'][player] == 0:
                            del standings['total'][player]
                del standings['rounds'][puzzle_name]

                with open(tournament_dir / 'standings.json', 'w', encoding='utf-8') as f:
                    json.dump(standings, f, ensure_ascii=False, indent=4)

            # Remove the round directory and metadata
            shutil.rmtree(round_dir)
            del tournament_metadata['rounds'][puzzle_name]

            with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            # Cancel and remove the relevant BG announcement task for the puzzle if any
            if puzzle_name in self.round_start_tasks:
                self.round_start_tasks[puzzle_name].cancel()
                del self.round_start_tasks[puzzle_name]
            elif puzzle_name in self.round_results_tasks:
                self.round_results_tasks[puzzle_name].cancel()
                del self.round_results_tasks[puzzle_name]

        reply = f"Successfully deleted {round_name}, `{puzzle_name}`"
        await (ctx.send(reply) if msg is None else msg.edit(content=reply))

    # TODO: tournament-update-puzzle? Probably not worth the complexity since depending on what updates old solutions
    #       may or not be invalidated. TO should just warn participants they'll need to resubmit, delete, re-add, and
    #       let the bot re-announce the puzzle

    # TODO: DDOS-mitigating measures such as:
    #       - maximum expected cycle count (set to e.g. 1 million unless specified otherwise for a puzzle) above which
    #         solutions are rejected and requested to be sent directly to the tournament host
    #       - limit user submissions to like 2 per minute

    # TODO: Accept blurb: https://discordpy.readthedocs.io/en/latest/ext/commands/commands.html#keyword-only-arguments
    @commands.command(name='tournament-submit', aliases=['ts'])
    #@commands.dm_only()  # TODO: Give the bot permission to delete !tournament-submit messages from public channels
                          #       since someone will inevitably forget to use DMs
    async def tournament_submit(self, ctx):
        """Submit the attached solution file to the tournament."""
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata()

        assert len(ctx.message.attachments) == 1, "Expected one attached solution file!"
        soln_bytes = await ctx.message.attachments[0].read()
        try:
            soln_str = soln_bytes.decode("utf-8").replace('\r\n', '\n')
        except UnicodeDecodeError as e:
            raise Exception("Attachment must be a plaintext file (containing a Community Edition export).") from e

        soln_strs = list(schem.Solution.split_solutions(soln_str))
        assert len(soln_strs) != 0, "Attachment does not contain SpaceChem solution string(s)"
        assert len(soln_strs) == 1 or is_tournament_host(ctx), "Expected only one solution in the attached file."

        reaction = '✅'
        for soln_str in soln_strs:
            msg = None
            try:
                level_name, author, expected_score, soln_name = schem.Solution.parse_metadata(soln_str)
                soln_descr = schem.Solution.describe(level_name, author, expected_score, soln_name)

                # Skip participant name checks for the TO backdoor
                if not is_tournament_host(ctx):
                    # Discord tag used to ensure no name collision errors or exploits, and so the host knows who to message in case
                    # of any issues
                    discord_tag = str(ctx.message.author)  # e.g. <username>#1234. Guaranteed to be unique

                    # Register this discord_tag: author_name mapping if it is not already registered
                    # If the solution's author_name conflicts with that of another player, request they change it
                    # If the author_name conflicts with that already submitted by this discord_id, warn them but proceed as if
                    # they purposely renamed themselves
                    with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
                        participants = json.load(f)

                    if discord_tag in participants:
                        if author != participants[discord_tag]:
                            # TODO: Could allow name changes but it would be a lot of work and potentially confusing for the
                            #       other participants, probably should only do this case-by-case and manually
                            raise ValueError(f"Given author name `{author}` doesn't match your prior submissions':"
                                             + f" `{participants[discord_tag]}`; please talk to the tournament host if you would"
                                             + " like a name change.")
                    else:
                        # First submission
                        if author in participants.values():
                            raise PermissionError(f"Solution author name `{author}` is already in use by another participant,"
                                                  + " please choose another (or login to the correct discord account).")

                        participants[discord_tag] = author

                    with open(tournament_dir / 'participants.json', 'w', encoding='utf-8') as f:
                        json.dump(participants, f, ensure_ascii=False, indent=4)

                # Since otherwise future puzzle names could in theory be searched for, make sure we return the same message
                # whether a puzzle does not exist or is not yet open for submissions
                unknown_level_exc = ValueError(f"No active tournament level `{level_name}`; ensure the first line of your solution"
                                               + " has the correct level name or check the start/end dates of the given puzzle.")
                if level_name not in tournament_metadata['rounds']:
                    raise unknown_level_exc
                round_metadata = tournament_metadata['rounds'][level_name]

                round_dir = tournament_dir / round_metadata['dir']

                # Check that the message was sent during the round's submission period
                if ctx.message.edited_at is not None:
                    # Cover any possible late-submission exploits if we ever allow bot to respond to edits
                    msg_time = ctx.message.edit_at.replace(tzinfo=timezone.utc)
                else:
                    msg_time = ctx.message.created_at.replace(tzinfo=timezone.utc)

                if msg_time < datetime.fromisoformat(round_metadata['start']):
                    raise unknown_level_exc
                elif msg_time > datetime.fromisoformat(round_metadata['end']):
                    raise Exception(f"Submissions for `{level_name}` have closed.")

                # TODO: Check if the tournament host set a higher max submission cycles value, otherwise default to e.g.
                #       10,000,000 and break here if that's violated

                with self.puzzle_submission_locks[level_name]:
                    level = self.get_level(round_dir)

                    # Verify the solution
                    # TODO: Provide seconds or minutes ETA based on estimate of 2,000,000 cycles / min (/ reactor?)
                    msg = await ctx.send(f"Running {soln_descr}, this should take < 30s barring an absurd cycle count...")

                    solution = schem.Solution(level, soln_str)

                    # Call the SChem validator in a thread so the bot isn't blocked
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(thread_pool_executor, solution.validate)

                    # TODO: if metric uses 'outputs' as a var, we should instead catch any run errors (or just PauseException,
                    #       to taste) and pass the post-run solution object to eval_metric regardless

                    # Calculate the solution's metric score
                    metric = round_metadata['metric']
                    soln_metric_score = eval_metric(solution, metric)

                    reply = f"Successfully validated {soln_descr}, metric score: {format_metric(soln_metric_score, decimals=3)}"

                    # Update solutions.txt
                    with open(round_dir / 'solutions.txt', 'r', encoding='utf-8') as f:
                        solns_str = f.read()

                    new_soln_strs = []
                    for cur_soln_str in schem.Solution.split_solutions(solns_str):
                        _, cur_author, _, _ = schem.Solution.parse_metadata(cur_soln_str)
                        if cur_author == author:
                            # Warn the user if their submission regresses the metric score
                            # (we will still allow the submission in case they wanted to submit something sub-optimal for
                            #  style/meme/whatever reasons)
                            # Note: This re-does the work of calculating the old metric but is simpler and allows the TO to
                            #       modify the metric after the puzzle opens if necessary
                            old_metric_score = eval_metric(schem.Solution(level, cur_soln_str), metric)
                            if soln_metric_score > old_metric_score:
                                reply += "\nWarning: This solution regresses your last submission's metric score, previously: " \
                                         + format_metric(old_metric_score, decimals=3)
                                if reaction == '✅':
                                    reaction = '⚠'
                        else:
                            new_soln_strs.append(cur_soln_str)

                    new_soln_strs.append(soln_str)

                    with open(round_dir / 'solutions.txt', 'w', encoding='utf-8') as f:
                        # Make sure not to write windows newlines or python will double the carriage returns
                        f.write('\n'.join(new_soln_strs))

                    # TODO: Update submissions_history.txt with time, name, score, and blurb

                    await msg.edit(content=reply)
            except Exception as e:
                reaction = '❌'
                print(f"{type(e).__name__}: {e}")
                # Replace the 'Running...' message if it got that far
                if msg is not None:
                    await msg.edit(content=f"{type(e).__name__}: {e}")
                else:
                    await ctx.send(f"{type(e).__name__}: {e}")

        await ctx.message.add_reaction(reaction)

    # TODO: Accept blurb: https://discordpy.readthedocs.io/en/latest/ext/commands/commands.html#keyword-only-arguments
    @commands.command(name='tournament-submit-fun', aliases=['tsf', 'tournament-submit-non-scoring', 'tsns'])
    #@commands.dm_only()  # TODO: Give the bot permission to delete !tournament-submit messages from public channels
    #       since someone will inevitably forget to use DMs
    async def tournament_submit_fun(self, ctx):
        """Submit the attached solution file to the tournament."""
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata()

        assert len(ctx.message.attachments) == 1, "Expected one attached solution file!"
        soln_bytes = await ctx.message.attachments[0].read()
        try:
            soln_str = soln_bytes.decode("utf-8").replace('\r\n', '\n')
        except UnicodeDecodeError as e:
            raise Exception("Attachment must be a plaintext file (containing a Community Edition export).") from e

        level_name, author, expected_score, soln_name = schem.Solution.parse_metadata(soln_str)
        soln_descr = schem.Solution.describe(level_name, author, expected_score, soln_name)

        # Discord tag used to ensure no name collision errors or exploits, and so the host knows who to message in case
        # of any issues
        discord_tag = str(ctx.message.author)  # e.g. <username>#1234. Guaranteed to be unique

        # Register this discord_tag: author_name mapping if it is not already registered
        # If the solution's author_name conflicts with that of another player, request they change it
        # If the author_name conflicts with that already submitted by this discord_id, warn them but proceed as if
        # they purposely renamed themselves
        with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
            participants = json.load(f)

        if discord_tag in participants:
            if author != participants[discord_tag]:
                # TODO: Could allow name changes but it would be a lot of work and potentially confusing for the
                #       other participants, probably should only do this case-by-case and manually
                raise ValueError(f"Given author name `{author}` doesn't match your prior submissions':"
                                 + f" `{participants[discord_tag]}`; please talk to the tournament host if you would"
                                 + " like a name change.")
        else:
            # First submission
            if author in participants.values():
                raise PermissionError(f"Solution author name `{author}` is already in use by another participant,"
                                      + " please choose another (or login to the correct discord account).")

            participants[discord_tag] = author

        with open(tournament_dir / 'participants.json', 'w', encoding='utf-8') as f:
            json.dump(participants, f, ensure_ascii=False, indent=4)

        # Since otherwise future puzzle names could in theory be searched for, make sure we return the same message
        # whether a puzzle does not exist or is not yet open for submissions
        unknown_level_exc = ValueError(f"No active tournament level `{level_name}`; ensure the first line of your solution"
                                       + " has the correct level name or check the start/end dates of the given puzzle.")
        if level_name not in tournament_metadata['rounds']:
            raise unknown_level_exc
        round_metadata = tournament_metadata['rounds'][level_name]

        round_dir = tournament_dir / round_metadata['dir']

        # Check that the message was sent during the round's submission period
        if ctx.message.edited_at is not None:
            # Cover any possible late-submission exploits if we ever allow bot to respond to edits
            msg_time = ctx.message.edit_at.replace(tzinfo=timezone.utc)
        else:
            msg_time = ctx.message.created_at.replace(tzinfo=timezone.utc)

        if msg_time < datetime.fromisoformat(round_metadata['start']):
            raise unknown_level_exc
        elif msg_time > datetime.fromisoformat(round_metadata['end']):
            raise Exception(f"Submissions for `{level_name}` have closed.")

        # TODO: Check if the tournament host set a higher max submission cycles value, otherwise default to e.g.
        #       10,000,000 and break here if that's violated

        with self.puzzle_submission_locks[level_name]:
            level = self.get_level(round_dir)

            # Verify the solution
            # TODO: Provide seconds or minutes ETA based on estimate of 2,000,000 cycles / min (/ reactor?)
            msg = await ctx.send(f"Running {soln_descr}, this should take < 30s barring an absurd cycle count...")

            solution = schem.Solution(level, soln_str)

            # Call the SChem validator in a thread so the bot isn't blocked
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(thread_pool_executor, solution.validate)

            reply = f"Added non-scoring submission {soln_descr}"

            # Add to solutions_fun.txt, replacing any existing solution by this author if it has the same solution name
            with open(round_dir / 'solutions_fun.txt', 'r', encoding='utf-8') as f:
                solns_str = f.read()

            new_soln_strs = []
            for cur_soln_str in schem.Solution.split_solutions(solns_str):
                _, cur_author, _, cur_soln_name = schem.Solution.parse_metadata(cur_soln_str)
                if cur_author == author and cur_soln_name == soln_name:
                    if not soln_name:
                        reply += "\nWarning: Solution has no name, and replaces your previous unnamed fun submission." \
                                 + " Consider naming your fun submissions for readability and to submit multiple of them!"
                    else:
                        reply += "\nReplaces previous fun submission of same name."
                else:
                    new_soln_strs.append(cur_soln_str)

            new_soln_strs.append(soln_str)

            with open(round_dir / 'solutions_fun.txt', 'w', encoding='utf-8') as f:
                # Make sure not to write windows newlines or python will double the carriage returns
                f.write('\n'.join(new_soln_strs))

        # TODO: Update submissions_history.txt with time, name, score, and blurb

        await ctx.message.add_reaction('✅')
        await msg.edit(content=reply)

    @commands.command(name='tournament-submissions-list', aliases=['tsl', 'tournament-list-submissions', 'tls'])
    #@commands.dm_only()  # Prevent public channel spam and make sure TO can't accidentally leak current round results
    async def tournament_list_submissions(self, ctx, *, round_or_puzzle_name=None):
        """List your puzzle submissions.

        If round/puzzle not specified, shows only currently-active puzzle submissions.

        round_or_puzzle_name: (Case-insensitive) If provided, show only your submissions
                              to the specified round/puzzle. May be a past puzzle.
        """
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=False)
        player_name = self.get_player_name(tournament_dir, ctx.message.author, missing_ok=False)

        reply = ""

        if round_or_puzzle_name is None:
            puzzle_names = [puzzle_name for puzzle_name, round_metadata in tournament_metadata['rounds'].items()
                            if 'start_post' in round_metadata and 'end_post' not in round_metadata]
        else:
            puzzle_names = [self.get_puzzle_name(tournament_metadata, round_or_puzzle_name,
                                                 is_host=False, missing_ok=False)]

        for puzzle_name in puzzle_names:
            round_metadata = tournament_metadata['rounds'][puzzle_name]
            round_dir = tournament_dir / round_metadata['dir']

            indent = ''
            if round_or_puzzle_name is None:
                reply += f"\n**{round_metadata['round_name']}, {puzzle_name}**:"
                indent = '    '

            # Check for scoring solution
            with open(round_dir / 'solutions.txt', 'r', encoding='utf-8') as f:
                solns_str = f.read()

            has_scoring = False
            for soln_str in schem.Solution.split_solutions(solns_str):
                _, cur_author, score, soln_name = schem.Solution.parse_metadata(soln_str)
                if cur_author == player_name:
                    has_scoring = True
                    level = self.get_level(round_dir)
                    reply += f"\n{indent}Scoring submission: {score}"
                    if soln_name is not None:
                        reply += ' ' + soln_name
                    reply += f", Metric Score: {eval_metric(schem.Solution(level, soln_str), round_metadata['metric'])}"
                    break
            if not has_scoring:
                reply += f"\n{indent}No scoring submission."

            # Check for non-scoring solutions
            with open(round_dir / 'solutions_fun.txt', 'r', encoding='utf-8') as f:
                fun_solns_str = f.read()

            fun_soln_lines = []
            for soln_str in schem.Solution.split_solutions(fun_solns_str):
                _, cur_author, score, soln_name = schem.Solution.parse_metadata(soln_str)
                if cur_author == player_name:
                    line = f"    {score}"
                    if soln_name is not None:
                        line += ' ' + soln_name
                    fun_soln_lines.append(line)
            if fun_soln_lines:
                reply += f"\n{indent}Non-scoring submissions:\n" + '\n'.join(fun_soln_lines)

        if not reply:
            await ctx.send("No active puzzles!")
        else:
            await ctx.send(reply.strip())  # Quick hack to remove newline prefix

    @commands.command(name='tournament-submission-fun-remove', aliases=['tournament-submission-non-scoring-remove',
                                                                        'tournament-remove-fun-submission',
                                                                        'tournament-remove-non-scoring-submission'])
    #@commands.dm_only()  # Prevent public channel spam and make sure TO can't accidentally leak current round results
    async def tournament_remove_fun_submission(self, ctx, round_or_puzzle_name, *, soln_name=None):
        """Remove a non-scoring submission to the given round/puzzle"""
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=False)
        puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name, is_host=False, missing_ok=False)
        player_name = self.get_player_name(tournament_dir, ctx.message.author, missing_ok=False)

        # If the user passes an empty string, interpret it to an unnamed solution
        if not soln_name:
            soln_name = None

        round_metadata = tournament_metadata['rounds'][puzzle_name]
        round_dir = tournament_dir / round_metadata['dir']

        # Prevent removal from an round whose results have already been published
        if 'end_post' in round_metadata:
            raise ValueError("Cannot remove submission from already-closed round!")

        with self.puzzle_submission_locks[puzzle_name]:
            with open(round_dir / 'solutions_fun.txt', 'r', encoding='utf-8') as f:
                solns_str = f.read()

            new_soln_strs = []
            reply = None
            for cur_soln_str in schem.Solution.split_solutions(solns_str):
                _, cur_author, score, cur_soln_name = schem.Solution.parse_metadata(cur_soln_str)
                if cur_author == player_name and cur_soln_name == soln_name:
                    if reply is not None:
                        print(f"Error: multiple non-scoring solutions to {puzzle_name} by {player_name} have same name {soln_name}")

                    if soln_name is None:
                        reply = f"Removed unnamed non-scoring solution: {score}"
                    else:
                        reply = f"Removed non-scoring solution: {score}, {soln_name}"
                else:
                    new_soln_strs.append(cur_soln_str)

            if reply is None:
                if soln_name is None:
                    raise ValueError("No unnamed non-scoring solution found; add solution name argument"
                                     + f" or double-check `!tournament-list-submissions {puzzle_name}`")

                raise ValueError(f"No non-scoring solution named `{soln_name}` found;"
                                 + f" double-check `!tournament-list-submissions {puzzle_name}`")

            with open(round_dir / 'solutions_fun.txt', 'w', encoding='utf-8') as f:
                # Make sure not to write windows newlines or python will double the carriage returns
                f.write('\n'.join(new_soln_strs))

        await ctx.send(reply)

    # TODO tournament-name-change

    @commands.command(name='tournament-info', aliases=['ti'])
    #@commands.dm_only()  # Prevent public channel spam and make sure TO can't accidentally leak current round results
    async def tournament_info(self, ctx, *, round_or_puzzle_name=None):
        """Info on the tournament or specified round/puzzle.

        round_or_puzzle_name: (Case-insensitive) Return links to the matching
                              puzzle's announcement (/ results if available) posts.
                              If not specified, show all puzzle announcement links
                              and current tournament standings.
        """
        is_host = is_tournament_host(ctx)
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=is_host)

        if round_or_puzzle_name is None:
            # Set up an embed listing all the rounds and their announcement messsages
            embed = discord.Embed(title=tournament_metadata['name'], description="")

            if 'start_post' in tournament_metadata:
                embed.description += f"[Announcement]({tournament_metadata['start_post']})"
            else:
                embed.description += f"Start: {self.format_tournament_datetime(tournament_metadata['start'])}" \
                                     + f" | End: {self.format_tournament_datetime(tournament_metadata['end'])}"

            embed.description += "\n**Rounds**:"
            for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                if 'start_post' in round_metadata:
                    embed.description += f"\n{round_metadata['round_name']}, {puzzle_name}:" \
                                         + f" [Announcement]({round_metadata['start_post']})"
                    if 'end_post' in round_metadata:
                        embed.description += f" | [Results]({round_metadata['end_post']})"
                elif is_host:
                    # Allow the TO to see schedule info on upcoming puzzles
                    embed.description += f"\n{round_metadata['round_name']}, {puzzle_name}:" \
                                         + f" Start: {self.format_tournament_datetime(round_metadata['start'])}" \
                                         + f" | End: {self.format_tournament_datetime(round_metadata['end'])}"

            # Create a standings table (in chunks under discord's char limit as needed)
            title_line = "**Standings**\n"
            standings_table_chunks = split_by_char_limit(self.standings_str(tournament_dir),
                                                         1999 - len(title_line) - 8)  # -8 for table backticks/newlines
            standings_msgs = [f"```\n{s}\n```" for s in standings_table_chunks]
            standings_msgs[0] = title_line + standings_msgs[0]

            # Send all the messages
            await ctx.send(embed=embed)
            for standings_msg in standings_msgs:
                await ctx.send(standings_msg)

            return

        # Convert to puzzle name
        puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name, is_host=is_host, missing_ok=False)
        round_metadata = tournament_metadata['rounds'][puzzle_name]

        if is_host and 'start_post' not in round_metadata:
            # If this is the host checking an unannounced puzzle, simply preview the announcement post for them
            await ctx.send(f"On {self.format_tournament_datetime(round_metadata['start'])} the following announcement will be sent:")
            embed, attachment = self.round_announcement(tournament_dir, tournament_metadata, puzzle_name)
            await ctx.send(embed=embed, file=attachment)
            return

        embed = discord.Embed(title=f"{round_metadata['round_name']}, {puzzle_name}",
                              description=f"[Announcement]({round_metadata['start_post']})")

        # Prevent non-TO users from accessing rounds that haven't ended or that the bot hasn't announced the results of yet
        if 'end_post' in round_metadata:
            embed.description += f" | [Results]({round_metadata['end_post']})"

        await ctx.send(embed=embed)

        # If this is the TO, preview the results post for them (in separate msgs so the embed goes on top)
        if is_host and not 'end_post' in round_metadata:
            await ctx.send(f"On {self.format_tournament_datetime(round_metadata['end'])} the following announcement will be sent:")

            # Send each of the sub-2000 char announcement messages, adding the attachments to the last one
            msg_strings, attachments, _ = self.round_results_announcement_and_standings_change(tournament_dir, tournament_metadata, puzzle_name)
            for i, msg_string in enumerate(msg_strings):
                if i < len(msg_strings) - 1:
                    await ctx.send(msg_string)
                else:
                    await ctx.send(msg_string, files=attachments)

    def tournament_announcement(self, tournament_metadata):
        """Return the tournament announcement text."""
        announcement = f"**Announcing the {tournament_metadata['name']}**"
        announcement += f"\nEnd date: {self.format_tournament_datetime(tournament_metadata['end'])}"

        return announcement

    def round_announcement(self, tournament_dir, tournament_metadata, puzzle_name,
                           level_code=None, attachment=None):
        """Helper to announce_round_start for creating the announcement msg, also used for the TO to preview.
        Return the announcement's embed and puzzle file.
        """
        round_metadata = tournament_metadata['rounds'][puzzle_name]

        if attachment is None:
            round_dir = tournament_dir / round_metadata['dir']

            puzzle_file = next(round_dir.glob('*.puzzle'), None)
            if puzzle_file is None:
                raise FileNotFoundError(f"{round_metadata['round_name']} puzzle file not found")

            attachment = discord.File(str(puzzle_file), filename=puzzle_file.name)
            with open(puzzle_file, 'r', encoding='utf-8') as pf:
                level_code = pf.read()  # Note: read() converts any windows newlines to unix newlines

        single_line_level_code = level_code.replace('\n', '')

        # Discord's embeds seem to be the only way to do a hyperlink to hide the giant puzzle preview link
        # TODO: description=flavour_text
        embed = discord.Embed(author=tournament_metadata['name'],
                              title=f"**Announcing {round_metadata['round_name']}, {puzzle_name}!**")
        embed.add_field(name='Preview',
                        value=f"[Coranac Site]({CORANAC_SITE}?code={single_line_level_code})",
                        inline=True)
        embed.add_field(name='Metric', value=f"`{round_metadata['metric']}`", inline=True)
        embed.add_field(name='Points', value=round_metadata['points'], inline=True)

        # Make the ISO datetime string friendlier-looking (e.g. no +00:00) or indicate puzzle is tournament-long
        round_end = self.format_tournament_datetime(round_metadata['end'])
        if round_metadata['end'] == tournament_metadata['end']:
            round_end += " (Tournament Close)"
        embed.add_field(name='Deadline', value=round_end, inline=True)

        # TODO: Add @tournament or something that notifies people who opt-in, preferably updateable by bot

        return embed, attachment

    async def wait_until(self, dt):
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

    async def announce_tournament_start(self, tournament_metadata):
        """Wait until the tournament start date and then announce it."""
        # TODO: Try/except/print in these background announcement tasks is ugly af, find a better way
        try:
            assert 'start_post' not in tournament_metadata, "Tournament has already been announced!"

            # Wait until the tournament start time
            start = tournament_metadata['start']
            await self.wait_until(datetime.fromisoformat(start))

            await self.bot.wait_until_ready()  # Looks awkward but apparently get_channel can return None if bot isn't ready
            channel = self.bot.get_channel(ANNOUNCEMENTS_CHANNEL_ID)

            async with self.tournament_metadata_write_lock:
                # Reread the tournament metadata since it may have changed
                tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)
                assert tournament_metadata['start'] == start, \
                    "Tournament start changed but original announcement task was not cancelled"
                assert 'start_post' not in tournament_metadata, \
                    "Tournament was announced while announcement task was still scheduled"

                print("Announcing tournament")
                msg = await channel.send(self.tournament_announcement(tournament_metadata))
                tournament_metadata['start_post'] = msg.jump_url

                with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                    json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

                # Schedule round start announcements
                # Only scheduling these after the tournament start announcement ensures that any puzzle starting at the
                # same time as the tournament (e.g. test puzzle) will not be announced before the tournament itself
                for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                    self.round_start_tasks[puzzle_name] = self.bot.loop.create_task(self.announce_round_start(puzzle_name, round_metadata))

                # Schedule the tournament results task
                self.tournament_results_task = self.bot.loop.create_task(self.announce_tournament_results(tournament_metadata))

                # Remove this task
                self.tournament_start_task = None
        except Exception as e:
            print(e)

    async def announce_round_start(self, puzzle_name, round_metadata):
        """Wait until the round start date and then announce it."""
        try:
            assert 'start_post' not in round_metadata, "Round has already been announced!"

            # Wait until the round start time
            start = round_metadata['start']
            await self.wait_until(datetime.fromisoformat(start))

            await self.bot.wait_until_ready()  # Looks awkward but apparently get_channel can return None if bot isn't ready
            channel = self.bot.get_channel(ANNOUNCEMENTS_CHANNEL_ID)

            async with self.tournament_metadata_write_lock:
                # Reread the tournament metadata since it may have changed
                tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)
                round_metadata = tournament_metadata['rounds'][puzzle_name]
                assert round_metadata['start'] == start, \
                    "Round start changed but original announcement task was not cancelled"
                assert 'start_post' not in round_metadata, \
                    "Round was announced while announcement task was still scheduled"

                print(f"Announcing {puzzle_name} start")
                embed, attachment = self.round_announcement(tournament_dir, tournament_metadata, puzzle_name)
                msg = await channel.send(embed=embed, file=attachment)

                # Keep the link to the original announcement post for !tournament-info. We can also check this to know
                # whether we've already done an announcement post
                round_metadata['start_post'] = msg.jump_url

                with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                    json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            # Create a submission lock for the puzzle and schedule the round results task
            self.puzzle_submission_locks[puzzle_name] = PuzzleSubmissionsLock()
            self.round_results_tasks[puzzle_name] = self.bot.loop.create_task(self.announce_round_results(puzzle_name, round_metadata))

            # Remove this task
            del self.round_start_tasks[puzzle_name]
        except Exception as e:
            print(e)

    async def announce_round_results(self, puzzle_name, round_metadata):
        """Wait until the round end date and then announce its results."""
        try:
            assert 'end_post' not in round_metadata, "Round results have already been announced!"

            # Wait until the round start time + 5 seconds to ensure last-second submitters have grabbed the submission lock
            end = round_metadata['end']
            await self.wait_until(datetime.fromisoformat(end) + timedelta(seconds=5))

            await self.bot.wait_until_ready()  # Looks awkward but apparently get_channel can return None if bot isn't ready
            channel = self.bot.get_channel(ANNOUNCEMENTS_CHANNEL_ID)

            async with self.tournament_metadata_write_lock:
                # Reread the tournament metadata since it may have changed
                tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)
                round_metadata = tournament_metadata['rounds'][puzzle_name]
                assert round_metadata['end'] == end, \
                    "Round end changed but original results announcement task was not cancelled"
                assert 'end_post' not in round_metadata, \
                    "Round results were announced while results announcement task was still scheduled"

                print(f"Announcing {puzzle_name} results")
                await self.puzzle_submission_locks[puzzle_name].lock_and_wait_for_submitters()
                msg_strings, attachments, standings_delta = \
                    self.round_results_announcement_and_standings_change(tournament_dir, tournament_metadata, puzzle_name)

                # Increment the tournament's standings
                with open(tournament_dir / 'standings.json', 'r', encoding='utf-8') as f:
                    standings = json.load(f)

                standings['rounds'][puzzle_name] = standings_delta
                for player, points in standings_delta.items():
                    if points > 0:
                        if player not in standings['total']:
                            standings['total'][player] = 0
                        standings['total'][player] += points

                with open(tournament_dir / 'standings.json', 'w', encoding='utf-8') as f:
                    json.dump(standings, f)

                # Send each of the sub-2000 char announcement messages, adding the attachments to the last one
                # Set the end post link to that of the first sent message
                for i, msg_string in enumerate(msg_strings):
                    if i < len(msg_strings) - 1:
                        msg = await channel.send(msg_string)
                    else:
                        msg = await channel.send(msg_string, files=attachments)

                    if i == 0:
                        round_metadata['end_post'] = msg.jump_url

                del self.puzzle_submission_locks[puzzle_name]

                with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                    json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            # Remove this task
            del self.round_results_tasks[puzzle_name]
        except Exception as e:
            print(e)

    async def announce_tournament_results(self, tournament_metadata):
        """Wait until the tournament end date and then announce its results."""
        try:
            assert 'end_post' not in tournament_metadata, "Tournament results have already been announced!"

            # Wait until the tournament end time
            end = tournament_metadata['end']
            await self.wait_until(datetime.fromisoformat(end))

            # Wait for any remaining puzzle rounds to be tallied by round results tasks (they take variable time
            # depending on any still-running submissions)
            # We'll know this is done when all round results tasks have been deleted
            while self.round_results_tasks:
                await asyncio.sleep(10)

            await self.bot.wait_until_ready()  # Looks awkward but apparently get_channel can return None if bot isn't ready
            channel = self.bot.get_channel(ANNOUNCEMENTS_CHANNEL_ID)

            async with self.tournament_metadata_write_lock:
                # Reread the tournament metadata since it may have changed
                tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)
                assert tournament_metadata['end'] == end, \
                    "Tournament end changed but original results announcement task was not cancelled"
                assert 'end_post' not in tournament_metadata, \
                    "Tournament results were announced while results announcement task was still scheduled"

                print("Announcing tournament results")
                title_line = f"**{tournament_metadata['name']} Results**\n"
                standings_table_chunks = split_by_char_limit(self.standings_str(tournament_dir),
                                                             1999 - len(title_line) - 8)  # -8 for table backticks/newlines
                msg_strings = [f"```\n{s}\n```" for s in standings_table_chunks]
                msg_strings[0] = title_line + msg_strings[0]

                # Send each of the sub-2000 char announcement messages, adding the attachments to the last one
                # Set the end post link to that of the first sent message
                for i, msg_string in enumerate(msg_strings):
                    msg = await channel.send(msg_string)

                    if i == 0:
                        tournament_metadata['end_post'] = msg.jump_url

                self.ACTIVE_TOURNAMENT_FILE.unlink()

                with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                    json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            # Remove this task
            self.tournament_results_task = None
        except Exception as e:
            print(e)

    @classmethod
    def ranking_str(cls, headers, rows, sort_idx=-1, desc=False, max_col_width=12):
        """Given an iterable of column headers and list of rows containing strings or numeric types, return a
        pretty-print table with appropriate column widths.
        If sort_idx isn't None, additionally add a ranking based on the given column idx to sort and desc (sort direction).
        E.g. Rank results for a puzzle or standings for the tournament.
        """
        # Sort and rank the rows
        if sort_idx is not None:
            headers = ['#'] + list(headers)
            last_score = None
            ranked_rows = []
            for i, row in enumerate(sorted(rows, key=lambda r: r[sort_idx], reverse=desc)):
                # Only increment rank if we didn't tie the previous score
                if row[sort_idx] != last_score:
                    rank = str(i + 1)  # We can go straight to string
                    last_score = row[sort_idx]

                ranked_rows.append([rank] + list(row))
        else:
            ranked_rows = rows

        # Prepend the header row and convert all given values to formatted strings
        formatted_rows = [headers] + [tuple(x if isinstance(x, str) else format_metric(x, decimals=3) for x in row)
                                      for row in ranked_rows]

        # Get the minimum width of each column
        min_widths = [min(max_col_width, max(map(len, col))) for col in zip(*formatted_rows)]  # Sorry future reader

        return '\n'.join('  '.join(s.ljust(min_widths[i]) for i, s in enumerate(row)) for row in formatted_rows)

    def round_results_announcement_and_standings_change(self, tournament_dir, tournament_metadata, puzzle_name):
        """Given tournament dir/metadata and a specified puzzle, return a list of strings of the announcement message(s)
        text for the puzzle results, a list of attachments, and a dict indicating the changes to the standings.
        """
        round_metadata = tournament_metadata['rounds'][puzzle_name]
        round_dir = tournament_dir / round_metadata['dir']

        attachments = []

        with open(round_dir / 'solutions.txt', 'r', encoding='utf-8') as sf:
            solns_str = sf.read()

        level = self.get_level(round_dir)

        solns_file = round_dir / 'solutions.txt'
        with open(solns_file, 'r', encoding='utf-8') as sf:
            solns_str = sf.read()
        attachments.append(discord.File(str(solns_file), filename=solns_file.name))

        solutions = [schem.Solution(level, soln_str) for soln_str in schem.Solution.split_solutions(solns_str)]

        # Calculate each score and the top score
        metric_scores_and_terms = [get_metric_and_terms(solution, round_metadata['metric']) for solution in solutions]
        min_metric_score = min(x[0] for x in metric_scores_and_terms) if metric_scores_and_terms else None

        # Sort by metric and add to the results string and player scores
        standings_scores = {}  # player_name: metric_score
        col_headers = []
        results = []
        for solution, (metric_score, term_values) in zip(solutions, metric_scores_and_terms):
            assert solution.author not in standings_scores, "solutions.txt unexpectedly contains duplicate player"

            # We'll add a standard-format score column directly and only add extra columns for non-standard metric terms
            for term_key in ('cycles', 'reactors', 'symbols'):
                if term_key in term_values:
                    del term_values[term_key]

            if not col_headers:
                col_headers = ['Name', 'Score'] + list(term_values.keys()) + ['Metric', 'Rel. Metric', 'Points']

            relative_metric = min_metric_score / metric_score
            points = round_metadata['points'] * relative_metric

            standings_scores[solution.author] = points
            results.append([solution.author, str(solution.expected_score)] + list(term_values.values())
                           + [metric_score, relative_metric, points])

        # TODO: Shouldn't need a solution to parse the header row; extract these from the metric
        if not solutions:
            col_headers = ('Player', 'Score', 'Metric', 'Rel. Metric', 'Points')

        # Embed not used as it is not wide enough for tables
        # Split the result table to fit under discord's 2000-character message limit
        title_line = f"**{round_metadata['round_name']} ({puzzle_name}) Results**\n"
        results_table_chunks = split_by_char_limit(self.ranking_str(col_headers, results, sort_idx=-3),
                                                   1999 - len(title_line) - 8)  # -8 for table backticks/newlines
        msg_strings = [f"```\n{s}\n```" for s in results_table_chunks]
        msg_strings[0] = title_line + msg_strings[0]

        # TODO: Add current overall tournament standings?

        # TODO: Also attach blurbs.txt

        # Add fun solutions if any
        # TODO: Add a second table listing the fun solutions by name, and excluding tournament points columns
        fun_solns_file = round_dir / 'solutions_fun.txt'
        with open(fun_solns_file, 'r', encoding='utf-8') as f:
            fun_solns_str = f.read().strip()

        if fun_solns_str:
            fun_solutions = [schem.Solution(level, s) for s in schem.Solution.split_solutions(fun_solns_str)]
            fun_col_headers = ('Player', 'Score', 'Solution Name')
            fun_table_rows = [(soln.author, soln.expected_score, soln.name if soln.name else '')
                              for soln in fun_solutions]

            title_line = "**Non-Scoring Submissions**\n"
            # Split the table up so it will just fit under discord's 2000-char msg limit even with the title prepended
            fun_table_chunks = split_by_char_limit(self.ranking_str(fun_col_headers, fun_table_rows, sort_idx=-3),
                                                   1999 - len(title_line) - 8)  # -8 for table backticks/newlines
            fun_table_msgs = [f"```\n{s}\n```" for s in fun_table_chunks]
            fun_table_msgs[0] = title_line + fun_table_msgs[0]
            msg_strings.extend(fun_table_msgs)

            attachments.append(discord.File(str(fun_solns_file), filename=fun_solns_file.name))

        return msg_strings, attachments, standings_scores

    def standings_str(self, tournament_dir):
        """Given a tournament's directory, return a string of the tournament standings"""
        with open(tournament_dir / 'standings.json', 'r', encoding='utf-8') as f:
            standings = json.load(f)

        return self.ranking_str(('Name', 'Score'), standings['total'].items(), desc=True)

bot.add_cog(Tournament(bot))

if __name__ == '__main__':
    bot.run(TOKEN)
