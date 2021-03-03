#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
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

# Tournaments structure:
# tournaments/
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
    if isinstance(error, commands.CommandNotFound):
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


class Tournament(commands.Cog):  # name="Help text name?"
    """Tournament Commands"""

    TOURNAMENTS_DIR = Path(__file__).parent / 'tournaments'  # Left relative so that filesystem paths can't leak into bot msgs
    ACTIVE_TOURNAMENT_FILE = TOURNAMENTS_DIR / 'active_tournament.txt'

    # Lock to ensure async calls don't overwrite each other
    # Should be used by any call that is writing to tournament_metadata.json. It is also assumed that no writer
    # calls await after having left the metadata in bad state. Given this, readers need not acquire the lock.
    tournament_metadata_write_lock = asyncio.Lock()

    def __init__(self, bot):
       self.bot = bot

       # Start bot's looping tasks
       self.announce_starts.start()
       self.announce_results.start()

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
        dt = datetime.fromisoformat(s)

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

    @commands.command(name='tournament-create')
    # TODO: Commented out all the public channel / permissions command lock decorators for debugging since doing so
    #       dynamically on --debug seems difficult - uncomment them!
    @commands.is_owner()  # TODO: @commands.has_role('tournament-host')
    async def tournament_create(self, ctx, name, start, end):
        """Create a tournament. There may only be one pending/active at a time.

        name: The tournament's official name, e.g. "2021 SpaceChem Tournament"
        start: The date that the bot will announce the tournament publicly and
               after which puzzle rounds may start.
        end: The date that the bot will announce the tournament results, after
             closing and tallying the results of any still-open puzzles.
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

        await ctx.send(f"Successfully created {repr(name)}")

    @commands.command(name='tournament-update')
    @commands.is_owner()  # TODO: @commands.has_role('tournament-host')
    #@commands.dm_only()
    async def tournament_update(self, ctx, new_name, new_start, new_end):
        updated_fields = []

        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            # Process dates while ignoring start being in the past, for the case where we didn't modify it
            new_start, new_end = self.process_tournament_dates(new_start, new_end, check_start_in_future=False)

            if new_start != tournament_metadata['start']:
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

        reply = f"Successfully updated tournament {', '.join(updated_fields)}."
        if modified_round_ends:
            reply += f"\nEnd date of round(s) `{'`, `'.join(modified_round_ends)}` updated to match new end date."

        await ctx.send(reply)

    # TODO: @commands.command(name='tournament-delete')  # Is this existing too dangerous?

    def get_puzzle_name(self, tournament_metadata, round_or_puzzle_name):
        """Given a string, return the puzzle name for any puzzle/round matching it case-insensitively, else None."""
        lower_name = round_or_puzzle_name.lower()
        for cur_puzzle_name, round_metadata in tournament_metadata['rounds'].items():
            if lower_name in (cur_puzzle_name.lower(), round_metadata['round_name'].lower()):
                return cur_puzzle_name

    # TODO: Puzzle flavour text
    @commands.command(name='tournament-add-puzzle')
    @commands.is_owner()  # TODO: @commands.has_role('tournament-host')
    #@commands.dm_only()
    async def tournament_add_puzzle(self, ctx, round_name, metric, total_points: int, start, end=None):
        """Add the attached puzzle file as a new round of the tournament.

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
        start: The datetime that round submissions open, in ISO format.
               If timezone unspecified, assumed to be UTC.
               E.g.: 2000-01-31, "2000-01-31 17:00:00", 2000-01-31T17:00:00-05:00.
        end: The datetime that round submissions close. Same format as `start`.
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

            # TODO: Track the history of each player's scores over time and do cool graphs of everyone's metrics going
            #       down as the deadline approaches!
            #       Can do like the average curve of everyone's scores over time and see how that curve varies by level
            #       Probably don't store every solution permanently to avoid the tournament.zip getting bloated but can
            #       at least keep the scores from replaced solutions.

            # TODO 2: Pareto frontier using the full submission history!

        await ctx.send(f"Successfully added {round_name} {level.name} to {tournament_metadata['name']}")

    @commands.command(name='tournament-delete-puzzle')
    @commands.is_owner()  # TODO: @commands.has_role('tournament-host')
    #@commands.dm_only()
    async def delete_puzzle(self, ctx, *, round_or_puzzle_name):
        """Delete the specified round/puzzle."""
        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            # Convert to puzzle name
            puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name)
            if puzzle_name is None:
                raise FileNotFoundError(f"No known puzzle/round ~~= `{round_or_puzzle_name}`")

            round_metadata = tournament_metadata['rounds'][puzzle_name]
            round_dir = tournament_dir / round_metadata['dir']
            round_name = round_metadata['round_name']

            # Ask for confirmation before deleting if the round start date has passed
            if datetime.now(timezone.utc).isoformat() > round_metadata['start']:
                timeout_seconds = 30
                msg = await ctx.send(f"This round's start date ({round_metadata['start']}) has already passed and"
                                     + " deleting it may remove player solutions. Are you sure you wish to delete it?"
                                     + f"\nReact to this message with ✅ within {timeout_seconds} seconds to proceed.")

                def check(reaction, user):
                    return reaction.message.id == msg.id and str(reaction.emoji) == '✅'

                try:
                    await self.bot.wait_for('reaction_add', timeout=timeout_seconds, check=check)
                except asyncio.TimeoutError:
                    await ctx.send('Puzzle not deleted!')
                    return

            shutil.rmtree(round_dir)
            del tournament_metadata['rounds'][puzzle_name]

            with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
                json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

        await ctx.send(f"Successfully deleted {round_name}, `{puzzle_name}`")

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
        """Submit the attached solution file to the matching tournament puzzle."""
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

        # TODO: Check if the tournament host set a higher max submission cycles value, otherwise default to e.g. 10,000,000
        #       and break here if that's violated

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

        # TODO: if metric uses 'outputs' as a var, we should instead catch any run errors (or just PauseException, to taste)
        #       and pass the post-run solution object to eval_metric regardless

        # Calculate the solution's metric score
        metric = round_metadata['metric']
        soln_metric_score = eval_metric(solution, metric)

        reply = f"Successfully validated {soln_descr}, metric score: {format_metric(soln_metric_score, decimals=1)}"

        # Update solutions.txt
        # TODO: Could maybe do a pure file-append on user's first submission to save computation, but probably won't
        #       be a bottleneck
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
                    reply += f"\nWarning: This solution regresses your last submission's metric score, previously: {old_metric_score}"
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
        """List info on the active tournament or if provided, the specified round/puzzle name."""
        is_host = await self.bot.is_owner(ctx.author)  # TODO: has_role('tournament-host')
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
                elif is_host:  # TODO: has_role('tournament-host')
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
        elif is_host:
            # If this is the TO, calculate and append the current round standings so they can keep an eye on its progress
            results_str, _ = self.get_round_results(tournament_dir, tournament_metadata, puzzle_name)

            await ctx.send(f"**Current Results**:\n```\n{results_str}\n```", embed=embed)
            return

        await ctx.send(embed=embed)

    def round_announcement(self, tournament_dir, tournament_metadata, puzzle_name):
        """Helper to announce_starts for creating the announcement msg, also used for the TO to preview.
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

        # Make the ISO datetime string friendlier-looking (e.g. no +00:00) or indicate puzzle is tournament-long
        round_end = self.format_tournament_datetime(round_metadata['end'])
        if round_metadata['end'] == tournament_metadata['end']:
            round_end += " (Tournament Close)"
        embed.add_field(name='Deadline', value=round_end, inline=True)

        # TODO: Add @tournament or something that notifies people who opt-in, preferably updateable by bot

        return embed, discord.File(str(puzzle_file), filename=puzzle_file.name)

    @tasks.loop(minutes=5)
    async def announce_starts(self):
        """Announce any rounds that just started or the tournament itself."""
        if not self.ACTIVE_TOURNAMENT_FILE.exists():
            return

        await self.bot.wait_until_ready()  # Looks awkward but apparently get_channel can return None if bot isn't ready
        channel = self.bot.get_channel(ANNOUNCEMENTS_CHANNEL_ID)

        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            # Announce the tournament if it just started
            if 'start_post' not in tournament_metadata:
                start_dt = datetime.fromisoformat(tournament_metadata['start'])
                seconds_since_start = (cur_time - start_dt).total_seconds()
                if 0 <= seconds_since_start <= 3600:
                    print("Announcing tournament")
                    announcement = f"Announcing the {tournament_metadata['name']}"
                    announcement += f"\nEnd date: {self.format_tournament_datetime(tournament_metadata['end'])}"

                    msg = await channel.send(announcement)
                    tournament_metadata['start_post'] = msg.jump_url
                elif seconds_since_start > 4500:
                    print(f"Error: `{tournament_metadata['name']}` has started but it was not announced within an hour")

            # Announce any round that started in the last hour and hasn't already been anounced
            cur_time = datetime.now(timezone.utc)
            for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                # Ignore round if it was already announced or didn't open for submissions in the last hour.
                # The hour limit is to ensure the bot doesn't spam too many announcements if something goes nutty,
                # while leaving some flex time in case the bot was down for some reason right after the puzzle started.
                start_dt = datetime.fromisoformat(round_metadata['start'])
                seconds_since_start = (cur_time - start_dt).total_seconds()
                if 'start_post' in round_metadata or not 0 <= seconds_since_start <= 3600:
                    # If we missed the hour limit but the puzzle was never announced, log a warning
                    if 'start_post' not in round_metadata and seconds_since_start > 3600:
                        # This will spam the log but only 96 times a day...
                        print(f"Error: Puzzle {puzzle_name} was not announced within an hour")
                    continue

                print(f"Announcing {puzzle_name} start")

                try:
                    embed, attachment = self.round_announcement(tournament_dir, tournament_metadata, puzzle_name)
                except Exception as e:
                    print(e)
                    continue

                msg = await channel.send(embed=embed, file=attachment)

                # Keep the link to the original announcement post for !tournament-info. We can also check this to know
                # whether we've already done an announcement post
                round_metadata['start_post'] = msg.jump_url

            with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as tm_f:
                json.dump(tournament_metadata, tm_f, ensure_ascii=False, indent=4)

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

    def get_round_results(self, tournament_dir, tournament_metadata, puzzle_name):
        """Given a solutions.txt, level, and metric, return both a formatted string of the players' ranked results, and
        a dict mapping each player's name to their gained tournament points.
        String format: Rank,Player,Score,<other metric terms>,Metric Score
        If puzzle points is included add: ,Rel. Metric,Points
        """
        round_metadata = tournament_metadata['rounds'][puzzle_name]
        round_dir = tournament_dir / round_metadata['dir']

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

        level = schem.Level(level_code)
        solutions = [schem.Solution(level, soln_str) for soln_str in schem.Solution.split_solutions(solns_str)]

        # TODO: Shouldn't need a solution to parse the header row, extract these from the metric
        if not solutions:
            return '#  Name  Cycles  Reactors  Symbols  Metric  Rel. Metric  Points', {}

        # Calculate each score and the top score
        metric_scores_and_terms = [get_metric_and_terms(solution, round_metadata['metric']) for solution in solutions]
        min_metric_score = min(x[0] for x in metric_scores_and_terms)

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

        return self.ranking_str(col_headers, results, sort_idx=-3), standings_scores

    def standings_str(self, tournament_dir):
        """Given a tournament's directory, return a string of the tournament standings"""
        with open(tournament_dir / 'standings.json', 'r', encoding='utf-8') as f:
            standings = json.load(f)

        return self.ranking_str(('Name', 'Score'), standings.items(), desc=True)

    @tasks.loop(minutes=5)
    async def announce_results(self):
        """Announce the results of any rounds that ended 15+ minutes ago, or of the tournament."""
        if not self.ACTIVE_TOURNAMENT_FILE.exists():
            return

        await self.bot.wait_until_ready()  # Looks awkward but apparently get_channel can return None if bot isn't ready
        channel = self.bot.get_channel(ANNOUNCEMENTS_CHANNEL_ID)

        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            # Announce the end of any round that ended over 15 min to 1:15 min ago and hasn't already has its end ennounced
            # The 15 minute delay is to give time for any last-minute submissions to be validated
            # TODO: Use some aync locks or some other proper way to ensure all submissions are done processing, without
            #       needing a hard-coded delay
            # The hour limit is just to make sure the bot doesn't spam old announcements if something goes nutty
            cur_time = datetime.now(timezone.utc)
            for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                end_dt = datetime.fromisoformat(round_metadata['end'])
                seconds_since_end = (cur_time - end_dt).total_seconds()
                if 'end_post' in round_metadata or not 900 <= seconds_since_end <= 4500: # 15 min to 1 hour 15 min
                    # If the hour announcement window has passed and the results post was never made, log a warning
                    if 'end_post' not in round_metadata and seconds_since_end > 4500:
                        # This will spam the log but only 96 times a day... indefinitely
                        print(f"Error: Puzzle {puzzle_name} has ended but results were not announced right afterward")
                    continue

                print(f"Announcing {puzzle_name} results")
                try:
                    results_str, standings_delta = self.get_round_results(tournament_dir, tournament_metadata, puzzle_name)
                except Exception as e:
                    print(e)
                    continue

                # Increment the tournament's standings
                with open(tournament_dir / 'standings.json', 'r', encoding='utf-8') as f:
                    standings = json.load(f)

                for name, points in standings_delta.items():
                    if name not in standings:
                        standings[name] = 0
                    standings[name] += points

                with open(tournament_dir / 'standings.json', 'w', encoding='utf-8') as f:
                    json.dump(standings, f)

                # Embed doesn't seem to be wide enough for tables, use code block
                announcement = f"{round_metadata['round_name']} ({puzzle_name}) Results"
                announcement += f"\n```\n{results_str}\n```"

                # TODO: Add current overall tournament standings?

                # TODO: Also attach blurbs.txt

                msg = await channel.send(announcement, file=discord.File(str(solns_file), filename=solns_file.name))
                round_metadata['end_post'] = msg.jump_url

            # If the tournament has ended, also announce the tournament results
            end_dt = datetime.fromisoformat(tournament_metadata['end'])
            seconds_since_end = (cur_time - end_dt).total_seconds()
            if 900 <= seconds_since_end <= 4500:
                print("Announcing tournament results")
                announcement = f"{tournament_metadata['name']} Results"
                announcement += f"\n```\n{self.standings_str(tournament_dir)}\n```"

                msg = await channel.send(announcement)
                tournament_metadata['end_post'] = msg.jump_url

                self.ACTIVE_TOURNAMENT_FILE.unlink()
            elif seconds_since_end > 4500:
                print(f"Error: `{tournament_metadata['name']}` has ended but results were not announced right afterward")

            with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as tm_f:
                json.dump(tournament_metadata, tm_f, ensure_ascii=False, indent=4)

bot.add_cog(Tournament(bot))

if __name__ == '__main__':
    bot.run(TOKEN)
