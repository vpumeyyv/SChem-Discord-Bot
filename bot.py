#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import shutil

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
import schem
from slugify import slugify

from metric import validate_metric, eval_metric, format_metric, get_metric_and_terms

load_dotenv()
TOKEN = os.getenv('SCHEM_BOT_DISCORD_TOKEN')
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
    if isinstance(error, commands.CommandNotFound) or isinstance(error, commands.CheckFailure):
        return  # Avoid logging errors when users put in invalid commands

    await ctx.send(str(error))  # Probably bad practice but it makes the commands' code nice...

@bot.command(name='run', aliases=['score', 'validate', 'check'])
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


class PuzzleSubmissionsLock:
    """Context manager which allows any number of submitters to a puzzle, until lock_and_wait_for_submitters is called,
    (`await puzzle_submission_lock.lock_and_wait_for_submitters()`), at which point new context
    requesters will receive an exception and the caller will wait for current submissions to finish.
    The lock will remain permanently locked once lock_and_wait_for_submitters() has been called (current
    users: puzzle results announcer and puzzle deleter).
    """
    def __init__(self):
        self.num_submitters = 0
        self.is_closed = False  # Raise exception in __aenter__ if set?
        self.is_blocker = False
        self.no_submissions_in_progress = asyncio.Event()
        self.no_submissions_in_progress.set()  # Set right away since no submitters to start

    async def lock_and_wait_for_submitters(self):
        """Permanently block new submitters from opening the context and wait for all current submitters to finish.
        May only be called once.
        """
        if self.is_closed:
            raise Exception("This puzzle has already been locked!")

        self.is_closed = True
        await self.no_submissions_in_progress.wait()  # Wait for all current submitters to exit their contexts

    def __enter__(self):
        """Raise an exception if lock_and_wait_for_submitters() has already been called, else register as a submitter
        and enter the context.
        """
        # Note that submissions check more precisely against puzzle end time, as long as we make it wait ~5 seconds to
        # ensure the async loop has time to call every pre-deadline submit, the results announcer should never be able
        # to block submitters (since they should check message time and grab the lock immediately)
        if self.is_closed:
            raise Exception("Puzzle has been closed or deleted")

        self.num_submitters += 1
        self.no_submissions_in_progress.clear()  # Anyone waiting for all submissions to complete will now be blocked

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Unregister as a submitter and indicate to any listeners if there are now no remaining submitters."""
        self.num_submitters -= 1
        if self.num_submitters == 0:
            # Indicate that any wait_and_block caller may open the context
            self.no_submissions_in_progress.set()


# Create a decorator for checking if a user has tournament-hosting permissions
# Unfortunately can't be a method of the Tournament cog since command.check doesn't pass self
def is_tournament_host(ctx):
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
    #         standings.json           -> name: score  # Edited by bot after each round
    #         bonus1_puzzleA/
    #         round1_puzzleB/
    #             puzzleB.puzzle
    #             solutions.txt
    #         round2_puzzleC/
    #         ...
    #     slugified_tournament_name_2/
    #     ...

    is_host = commands.check(is_tournament_host)  # Made a class var to reduce shadowing issues

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

            # Schedule the relevant tournament announcement if not yet done
            if 'start_post' not in tournament_metadata:
                self.tournament_start_task = self.bot.loop.create_task(self.announce_tournament_start(tournament_metadata))
            elif 'end_post' not in tournament_metadata:
                self.tournament_results_task = self.bot.loop.create_task(self.announce_tournament_results(tournament_metadata))

            for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                if 'start_post' not in round_metadata:
                    # If the puzzle has not been announced yet, schedule the announcement task
                    self.round_start_tasks[puzzle_name] = self.bot.loop.create_task(self.announce_round_start(puzzle_name, round_metadata))
                elif 'end_post' not in round_metadata:
                    # If the puzzle has opened but not ended, add a submissions lock and results announcement task
                    self.puzzle_submission_locks[puzzle_name] = PuzzleSubmissionsLock()
                    self.round_results_tasks[puzzle_name] = self.bot.loop.create_task(self.announce_round_results(puzzle_name, round_metadata))

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

    def format_tournament_datetime(self, s):
        """Return the given datetime string (expected to be UTC and as returned by datetime.isoformat()) in a more
        friendly format.
        """
        return ' '.join(s[:-9].split('T')) + ' UTC'  # Remove T and the seconds field, and replace '+00:00' with ' UTC'

    # Note: Command docstrings should be limited to ~80 characters to avoid ugly wraps in any reasonably-sized window

    def hosts(self):
        hosts_json_file = self.TOURNAMENTS_DIR / 'hosts.json'
        if not hosts_json_file.exists():
            return set()

        with open(hosts_json_file, encoding='utf-8') as f:
            return set(json.load(f)['hosts'])

    @commands.command(name='tournament-hosts')
    @is_host
    async def hosts_cmd(self, ctx):
        """List all tournament hosts."""
        await ctx.send(f"The following users have tournament-hosting permissions: {', '.join(self.hosts())}")

    @commands.command(name='add-tournament-host')
    @commands.is_owner()
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

    @commands.command(name='remove-tournament-host')
    @commands.is_owner()
    async def remove_tournament_host(self, ctx, user: discord.User):
        """Remove someone's tournament-hosting permissions."""
        discord_tag = str(user)  # e.g. <username>#1234. Guaranteed to be unique

        hosts = self.hosts()
        if discord_tag not in hosts:
            raise ValueError("Given user is not a tournament host")
        hosts.remove(discord_tag)

        with open(self.TOURNAMENTS_DIR / 'hosts.json', 'w', encoding='utf-8') as f:
            json.dump({'hosts': list(hosts)}, f)

        await ctx.send(f"{discord_tag} removed from tournament hosts.")

    @commands.command(name='tournament-create')
    @is_host
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
                json.dump({}, f, ensure_ascii=False, indent=4)

            # Schedule the tournament announcement
            self.tournament_start_task = self.bot.loop.create_task(self.announce_tournament_start(tournament_metadata))

        await ctx.send(f"Successfully created {repr(name)}")

    @commands.command(name='tournament-update')
    @is_host
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
                    raise ValueError(f"New start date is in past")

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
                for round_metadata in tournament_metadata['rounds'].values():
                    # Again all below date comparisons safe since everything is ISO and UTC format
                    if round_metadata['end'] == tournament_metadata['end']:
                        # Check round start isn't violated before we modify round end
                        if round_metadata['start'] >= new_end:
                            raise ValueError(f"New end date is before start of {repr(round_metadata['round_name'])}")

                        round_metadata['end'] = new_end
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
                json.dump(tournament_metadata, f)

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
    #                                                    # In any case tournament-update should besufficient for now

    def get_puzzle_name(self, tournament_metadata, round_or_puzzle_name):
        """Given a string, return the puzzle name for any puzzle/round matching it case-insensitively, else None."""
        lower_name = round_or_puzzle_name.lower()
        for cur_puzzle_name, round_metadata in tournament_metadata['rounds'].items():
            if lower_name in (cur_puzzle_name.lower(), round_metadata['round_name'].lower()):
                return cur_puzzle_name

    # TODO: Puzzle flavour text
    @commands.command(name='tournament-add-puzzle')
    @is_host
    async def tournament_add_puzzle(self, ctx, round_name, metric, total_points: float, start, end=None):
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
        total_points: # of points that the first place player will receive.
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
        if not puzzle_file.filename.endswith('.puzzle'):
            # TODO: Could fall back to slugify(level.name) or slugify(round_name) for the .puzzle file name if the
            #       extension doesn't match
            raise ValueError("Attached file should use the extension .puzzle")

        level_bytes = await puzzle_file.read()
        try:
            level_code = level_bytes.decode("utf-8")
        except UnicodeDecodeError as e:
            raise Exception("Attachment must be a plaintext file (containing a level export code).") from e
        level = schem.Level(level_code)

        validate_metric(metric)

        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            # Check if either of puzzle/round name are too similar to existing names of either type (since e.g.
            # tournament-info searches case-insensitively for either name)
            for name in (level.name, round_name):
                if self.get_puzzle_name(tournament_metadata, name) is not None:
                    raise ValueError(f"Puzzle/round with name ~= `{name}` already exists in the current tournament")
            round_dir_name = f'{slugify(round_name)}_{slugify(level.name)}'  # Human-friendly directory name

            # Validate start/end datetimes
            if end is None:
                end = tournament_metadata['end']

            start, end = self.process_tournament_dates(start, end)  # Format and based temporal sanity checks

            # Also check against the tournament start/end dates
            # String comparisons are safe here because all datetimes have been converted to ISO + UTC format
            if start < tournament_metadata['start']:
                raise ValueError(f"Round start time is before tournament start ({tournament_metadata['start']}).")
            elif end > tournament_metadata['end']:
                raise ValueError(f"Round end time is after tournament end ({tournament_metadata['end']}).")

            tournament_metadata['rounds'][level.name] = {'dir': round_dir_name,
                                                         'round_name': round_name,
                                                         'metric': metric,
                                                         'total_points': total_points,
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

            with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            # Schedule the round announcement
            self.round_start_tasks[level.name] = self.bot.loop.create_task(self.announce_round_start(level.name,
                                                                                                     tournament_metadata['rounds'][level.name]))

            # TODO: Track the history of each player's scores over time and do cool graphs of everyone's metrics going
            #       down as the deadline approaches!
            #       Can do like the average curve of everyone's scores over time and see how that curve varies by level
            #       Probably don't store every solution permanently to avoid the tournament.zip getting bloated but can
            #       at least keep the scores from replaced solutions.

            # TODO 2: Pareto frontier using the full submission history!

        await ctx.send(f"Successfully added {round_name} {level.name} to {tournament_metadata['name']}")

    @commands.command(name='tournament-delete-puzzle')
    @is_host
    async def delete_puzzle(self, ctx, *, round_or_puzzle_name):
        """Delete a round/puzzle."""
        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            # Convert to puzzle name
            puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name)
            if puzzle_name is None:
                raise FileNotFoundError(f"No known puzzle/round ~= `{round_or_puzzle_name}`")

            round_metadata = tournament_metadata['rounds'][puzzle_name]
            round_dir = tournament_dir / round_metadata['dir']
            round_name = round_metadata['round_name']

            msg = None

            # Ask for confirmation before deleting if the round start date has passed
            if datetime.now(timezone.utc).isoformat() > round_metadata['start']:
                timeout_seconds = 30
                warn_msg = await ctx.send(
                    f"Warning: This round's start date ({round_metadata['start']}) has already passed and"
                    + " deleting it will delete any player solutions. Are you sure you wish to continue?"
                    + f"\nReact to this message with ✅ within {timeout_seconds} seconds to delete anyway.")

                def check(reaction, user):
                    return reaction.message.id == warn_msg.id and str(reaction.emoji) == '✅'

                try:
                    await self.bot.wait_for('reaction_add', timeout=timeout_seconds, check=check)
                except asyncio.TimeoutError:
                    await ctx.send('Puzzle not deleted!')
                    return

                # TODO: Should deleting an already-closed puzzle be allowed?
                if puzzle_name in self.puzzle_submission_locks:
                    msg = await ctx.send(f"Waiting for any current submitters...")
                    await self.puzzle_submission_locks[puzzle_name].lock_and_wait_for_submitters()
                    del self.puzzle_submission_locks[puzzle_name]

            shutil.rmtree(round_dir)
            del tournament_metadata['rounds'][puzzle_name]

            with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            # Cancel and remove the relevant BG announcement task if the puzzle was not closed
            if 'start_post' not in round_metadata:
                self.round_start_tasks[puzzle_name].cancel()
                del self.round_start_tasks[puzzle_name]
            elif 'end_post' not in round_metadata:
                self.round_results_tasks[puzzle_name].cancel()
                del self.round_results_tasks[puzzle_name]

        await (ctx.send if msg is None else msg.edit)(f"Successfully deleted {round_name}, `{puzzle_name}`")

    # TODO: tournament-update-puzzle? Probably not worth the complexity since depending on what updates old solutions
    #       may or not be invalidated. TO should just warn participants they'll need to resubmit, delete, re-add, and
    #       let the bot re-announce the puzzle

    # TODO: DDOS-mitigating measures such as:
    #       - maximum expected cycle count (set to e.g. 1 million unless specified otherwise for a puzzle) above which
    #         solutions are rejected and requested to be sent directly to the tournament host
    #       - limit user submissions to like 2 per minute

    # TODO: Accept blurb: https://discordpy.readthedocs.io/en/latest/ext/commands/commands.html#keyword-only-arguments
    @commands.command(name='tournament-submit')
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

        # Cover any possible late-submission exploits if we ever allow bot to respond to edits
        if ctx.message.edited_at is not None:
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
            puzzle_file = next(round_dir.glob('*.puzzle'), None)
            if puzzle_file is None:
                print(f"Error: {round_dir} puzzle file not found!")
                raise FileNotFoundError("Round puzzle file not found; I seem to be experiencing an error.")

            with open(puzzle_file, 'r', encoding='utf-8') as f:
                level_code = f.read()

            # Verify the solution
            # TODO: Provide seconds or minutes ETA based on estimate of 2,000,000 cycles / min (/ reactor?)
            msg = await ctx.send(f"Running {soln_descr}, this should take < 30s barring an absurd cycle count...")

            level = schem.Level(level_code)
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
                _, cur_author, last_score, _ = schem.Solution.parse_metadata(cur_soln_str)
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
                else:
                    new_soln_strs.append(cur_soln_str)

            new_soln_strs.append(soln_str)

            with open(round_dir / 'solutions.txt', 'w', encoding='utf-8') as f:
                # Make sure not to write windows newlines or python will double the carriage returns
                f.write('\n'.join(new_soln_strs))

        # TODO: Update submissions_history.txt with time, name, score, and blurb

        await ctx.message.add_reaction('✅')
        await msg.edit(content=reply)

    # TODO tournament-name-change
    # TODO tournament-submit-fun

    @commands.command(name='tournament-info')
    #@commands.dm_only()  # Prevent public channel spam and make sure TO can't accidentally leak current round results
    async def tournament_info(self, ctx, *, round_or_puzzle_name=None):
        """Info on the tournament or specified round/puzzle."""
        is_host = is_tournament_host(ctx)
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=is_host)

        if round_or_puzzle_name is None:
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

            embed.description += f"\n**Standings**:\n```\n{self.standings_str(tournament_dir)}\n```"

            await ctx.send(embed=embed)
            return

        # Convert to puzzle name
        puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name)

        # Make sure we return the same error message whether the puzzle/round doesn't exist or user doesn't have
        # permission to see it, so future names can't be inferred
        puzzle_nonexistent_or_not_closed_exc = FileNotFoundError(f"No known puzzle/round ~= `{round_or_puzzle_name}`")
        if puzzle_name is None:
            raise puzzle_nonexistent_or_not_closed_exc

        round_metadata = tournament_metadata['rounds'][puzzle_name]

        if 'start_post' not in round_metadata:
            if not is_host:
                raise puzzle_nonexistent_or_not_closed_exc

            # If this is the host checking an unannounced puzzle, simply preview the announcement post for them
            reply = f"On {self.format_tournament_datetime(round_metadata['start'])} the following announcement will be sent:"
            embed, attachment = self.round_announcement(tournament_dir, tournament_metadata, puzzle_name)
            await ctx.send(reply, embed=embed, file=attachment)
            return

        embed = discord.Embed(title=f"{round_metadata['round_name']}, {puzzle_name}",
                              description=f"[Announcement]({round_metadata['start_post']})")

        # Prevent non-TO users from accessing rounds that haven't ended or that the bot hasn't announced the results of yet
        if 'end_post' in round_metadata:
            embed.description += f" | [Results]({round_metadata['end_post']})"

        await ctx.send(embed=embed)

        # If this is the TO, preview the results post for them (in a separate msg so the embed goes on top)
        if is_host and not 'end_post' in round_metadata:
            reply = f"On {self.format_tournament_datetime(round_metadata['end'])} the following announcement will be sent:"
            announcement, attachments, _ = self.round_results_announcement_and_standings_change(tournament_dir, tournament_metadata, puzzle_name)

            await ctx.send(announcement, files=attachment)

    def round_announcement(self, tournament_dir, tournament_metadata, puzzle_name):
        """Helper to announce_round_start for creating the announcement msg, also used for the TO to preview.
        Return the announcement's embed and puzzle file.
        """
        round_metadata = tournament_metadata['rounds'][puzzle_name]
        round_dir = tournament_dir / round_metadata['dir']

        puzzle_file = next(round_dir.glob('*.puzzle'), None)
        if puzzle_file is None:
            raise FileNotFoundError(f"{round_metadata['round_name']} puzzle file not found")

        with open(puzzle_file, 'r', encoding='utf-8') as pf:
            level_code = pf.read()  # Note: read() converts any windows newlines to unix newlines
        single_line_level_code = level_code.replace('\n', '')

        # Discord's embeds seem to be the only way to do a hyperlink to hide the giant puzzle preview link
        # TODO: description=flavour_text
        embed = discord.Embed(author=tournament_metadata['name'],
                              title=f"Announcing {round_metadata['round_name']}, {puzzle_name}!")
        embed.add_field(name='Preview',
                        value=f"[Coranac Site]({CORANAC_SITE}?code={single_line_level_code})",
                        inline=True)
        embed.add_field(name='Metric', value=round_metadata['metric'], inline=True)
        embed.add_field(name='Points', value=round_metadata['total_points'], inline=True)

        # Make the ISO datetime string friendlier-looking (e.g. no +00:00) or indicate puzzle is tournament-long
        round_end = self.format_tournament_datetime(round_metadata['end'])
        if round_metadata['end'] == tournament_metadata['end']:
            round_end += " (Tournament Close)"
        embed.add_field(name='Deadline', value=round_end, inline=True)

        # TODO: Add @tournament or something that notifies people who opt-in, preferably updateable by bot

        return embed, discord.File(str(puzzle_file), filename=puzzle_file.name)

    async def wait_until(self, dt):
        """Helper to async sleep until the given Datetime."""
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

            await asyncio.sleep(remaining_seconds)

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
                announcement = f"Announcing the {tournament_metadata['name']}"
                announcement += f"\nEnd date: {self.format_tournament_datetime(tournament_metadata['end'])}"

                msg = await channel.send(announcement)
                tournament_metadata['start_post'] = msg.jump_url

                with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                    json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            # Schedule the tournament results task
            self.tournament_results_task = self.bot.loop.create_task(self.announce_tournament_results(tournament_metadata))

            # Remove this task
            self.tournament_start_task = None
        except asyncio.CancelledError:  # TODO: Not needed in py3.8+ when CancelledError subclasses BaseException
            raise
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
        except asyncio.CancelledError:
            raise
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
                announcement, attachments, standings_delta = \
                    self.round_results_announcement_and_standings_change(tournament_dir, tournament_metadata, puzzle_name)

                # Increment the tournament's standings
                with open(tournament_dir / 'standings.json', 'r', encoding='utf-8') as f:
                    standings = json.load(f)

                for name, points in standings_delta.items():
                    if name not in standings:
                        standings[name] = 0
                    standings[name] += points

                with open(tournament_dir / 'standings.json', 'w', encoding='utf-8') as f:
                    json.dump(standings, f)

                msg = await channel.send(announcement, files=attachments)
                round_metadata['end_post'] = msg.jump_url

                del self.puzzle_submission_locks[puzzle_name]

                with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                    json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            # Remove this task
            del self.round_results_tasks[puzzle_name]
        except asyncio.CancelledError:
            raise
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
                announcement = f"{tournament_metadata['name']} Results"
                announcement += f"\n```\n{self.standings_str(tournament_dir)}\n```"

                msg = await channel.send(announcement)
                tournament_metadata['end_post'] = msg.jump_url

                self.ACTIVE_TOURNAMENT_FILE.unlink()

                with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                    json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

            # Remove this task
            self.tournament_results_task = None
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(e)

    def ranking_str(self, headers, rows, sort_idx=-1, desc=False, max_col_width=12):
        """Given an iterable of column headers and list of rows containing strings or numeric types, return a
        pretty-print table including rankings for each row based on the given column and sort direction.
        E.g. Rank results for a puzzle or standings for the tournament.
        """
        # Sort and rank the rows
        headers = ['#'] + list(headers)
        last_score = None
        ranked_rows = []
        for i, row in enumerate(sorted(rows, key=lambda r: r[sort_idx], reverse=desc)):
            # Only increment rank if we didn't tie the previous score
            if row[sort_idx] != last_score:
                rank = str(i + 1)  # We can go straight to string
                last_score = row[sort_idx]

            ranked_rows.append([rank] + list(row))

        # Prepend the header row and convert all given values to formatted strings
        formatted_rows = [headers] + [tuple(x if isinstance(x, str) else format_metric(x, decimals=3) for x in row)
                                      for row in ranked_rows]

        # Get the minimum width of each column (incl. rank)
        min_widths = [min(max_col_width, max(map(len, col))) for col in zip(*formatted_rows)]  # Sorry

        return '\n'.join('  '.join(s.ljust(min_widths[i]) for i, s in enumerate(row)) for row in formatted_rows)

    def round_results_announcement_and_standings_change(self, tournament_dir, tournament_metadata, puzzle_name):
        """Given tournament dir/metadata and a specified puzzle, return a string of the announcement message text for
        the puzzle results, a list of attachments, and a dict indicating the changes to the standings.
        """
        round_metadata = tournament_metadata['rounds'][puzzle_name]
        round_dir = tournament_dir / round_metadata['dir']

        attachments = []

        with open(round_dir / 'solutions.txt', 'r', encoding='utf-8') as sf:
            solns_str = sf.read()

        puzzle_file = next(round_dir.glob('*.puzzle'), None)
        if puzzle_file is None:
            raise FileNotFoundError(f"{round_metadata['round_name']} puzzle file not found")

        with open(puzzle_file, encoding='utf-8') as pf:
            level_code = pf.read()

        solns_file = round_dir / 'solutions.txt'
        with open(solns_file, 'r', encoding='utf-8') as sf:
            solns_str = sf.read()
        attachments.append(discord.File(str(solns_file), filename=solns_file.name))

        level = schem.Level(level_code)
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

            if not col_headers:
                col_headers = ['Name'] + list(term_values.keys()) + ['Metric', 'Rel. Metric', 'Points']

            relative_metric = min_metric_score / metric_score
            points = round_metadata['total_points'] * relative_metric

            standings_scores[solution.author] = points
            results.append([solution.author] + list(term_values.values()) + [metric_score, relative_metric, points])

        # TODO: Shouldn't need a solution to parse the header row; extract these from the metric
        if not solutions:
            col_headers = ('#', 'Name', 'Cycles', 'Reactors', 'Symbols', 'Metric', 'Rel. Metric', 'Points')

        # Embed doesn't seem to be wide enough for tables, use code block
        announcement = f"{round_metadata['round_name']} ({puzzle_name}) Results"
        announcement += f"\n```\n{self.ranking_str(col_headers, results, sort_idx=-3)}\n```"

        # TODO: Add current overall tournament standings?

        # TODO: Also attach blurbs.txt

        return announcement, attachments, standings_scores

    def standings_str(self, tournament_dir):
        """Given a tournament's directory, return a string of the tournament standings"""
        with open(tournament_dir / 'standings.json', 'r', encoding='utf-8') as f:
            standings = json.load(f)

        return self.ranking_str(('Name', 'Score'), standings.items(), desc=True)


bot.add_cog(Tournament(bot))

if __name__ == '__main__':
    bot.run(TOKEN)
