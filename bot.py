#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
import schem
from slugify import slugify

from metric import validate_metric, eval_metric

load_dotenv()
TOKEN = os.getenv('SCHEM_BOT_DISCORD_TOKEN')
ANNOUNCEMENTS_CHANNEL_ID = int(os.getenv('SCHEM_BOT_ANNOUNCEMENTS_CHANNEL_ID'))

SC_CHANNEL_ID = None  # TODO: Set this to the bot testing server channel for now
CORANAC_SITE = "https://www.coranac.com/spacechem/mission-viewer"

# TODO: Organize things to be able to use the same commands or code to run standalone puzzle challenges unrelated to
#       any tournament, e.g. puzzle-of-the-week/month, behaving like a standalone tournament round

# Tournaments structure:
# tournaments/
#     active_tournament.txt -> "slugified_tournament_name_1"
#     slugified_tournament_name_1/
#         tournament_metadata.json -> name, host, etc, + round dirs / metadata
#         participants.json        -> discord_tag: name (as it will appear in solution exports)
#         standings.csv  # Edited by bot after each round
#         bonus1_puzzleA/
#         round1_puzzleB/
#             puzzleB.puzzle
#             solutions.txt
#             results.csv  # Added after puzzle close
#         round2_puzzleC/
#         ...
#     slugified_tournament_name_2/
#     ...
TOURNAMENTS_DIR = Path(__file__).parent / 'tournaments'  # Left relative so that filesystem paths can't leak into bot msgs
ACTIVE_TOURNAMENT_FILE = TOURNAMENTS_DIR / 'active_tournament.txt'

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

    def __init__(self, bot):
       self.bot = bot

       # Start bot's looping tasks
       self.announce_tournament_round_start.start()
       self.announce_tournament_round_results.start()

    def get_active_tournament_dir_and_metadata(self):
        """Helper to fetch the active tournament directory and metadata."""
        if not ACTIVE_TOURNAMENT_FILE.exists():
            raise FileNotFoundError("No active tournament!")

        with open(ACTIVE_TOURNAMENT_FILE, 'r', encoding='utf-8') as f:
            tournament_dir = TOURNAMENTS_DIR / f.read().strip()

        with open(tournament_dir / 'tournament_metadata.json', 'r', encoding='utf-8') as f:
            tournament_metadata = json.load(f)

        return tournament_dir, tournament_metadata

    @commands.command(name='tournament-start')
    # TODO: Commented out all the public channel / permissions command lock decorators for debugging since doing so
    #       dynamically on --debug seems difficult - uncomment them!
    #@commands.is_owner()  # TODO: @commands.has_role('tournament-host')
    async def tournament_start(self, ctx, name):
        """Start a tournament with the given name.
        Only one tournament may run at a time.
        """
        tournament_dir_name = slugify(name)  # Convert to a valid directory name
        assert tournament_dir_name, f"Invalid tournament name {name}"

        TOURNAMENTS_DIR.mkdir(exist_ok=True)

        if ACTIVE_TOURNAMENT_FILE.exists():
            raise FileExistsError("There is already an active tournament.")

        tournament_dir = TOURNAMENTS_DIR / tournament_dir_name
        tournament_dir.mkdir(exist_ok=False)

        with open(ACTIVE_TOURNAMENT_FILE, 'w', encoding='utf-8') as f:
            f.write(tournament_dir_name)

        # Initialize tournament metadata, participants (discord_id: soln_author_name), and standings files
        tournament_metadata = {'name': name, 'host': ctx.message.author.name, 'rounds': {}}
        with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
            json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

        with open(tournament_dir / 'participants.json', 'w', encoding='utf-8') as f:
            json.dump({}, f, ensure_ascii=False, indent=4)

        (tournament_dir / 'standings.csv').touch()

        # Announce tournament
        # TODO: Tournament announcements should be made in #spacechem even if tournament host commands are sent via DM
        await ctx.send("Hey a tournament has been started and stuff")  # TODO


    @commands.command(name='tournament-end')
    #@commands.is_owner()  # TODO: @commands.has_role('tournament-host')
    async def tournament_end(self, ctx):
        """End the active tournament."""
        _, tournament_metadata = self.get_active_tournament_dir_and_metadata()

        # TODO: End all active rounds and tabulate their results
        for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
            if 'end_post' not in round_metadata:
                pass

        ACTIVE_TOURNAMENT_FILE.unlink()

        # TODO: Announce tournament results
        await ctx.send("Tournament's over my dudes")

    # TODO: Puzzle flavour text
    @commands.command(name='tournament-add-puzzle')
    #@commands.is_owner()  # TODO: @commands.has_role('tournament-host')
    #@commands.dm_only()
    async def tournament_add_puzzle(self, ctx, round_name, metric, total_points: int, start=None, end=None):
        """Add the attached puzzle file as a new round of the tournament.

        round_name: e.g. `"Round 1"` or `"Bonus 1"`.
        metric: The equation that will govern the raw score of a player's submission. A player's final score for the
                round will be the top metric score divided by this metric score.
                Allowed terms: `cycles`, `reactors`, `symbols`, `waldopath`, `waldos`, `bonders`
                E.g.: `"cycles + (0.1 * symbols) + (bonders^2)"`
                      Note the excess brackets since operator order isn't currently guaranteed to follow BEDMAS.
        total_points: Number of points that the first place player of the round will receive.
        start: The datetime that submissions to the round open, in ISO format. If timezone unspecified, assumed to be
               UTC.
               E.g.: `2000-01-31`, `"2000-01-31 17:00:00"`, `2000-01-31T17:00:00-05:00`.
               If excluded puzzle is open as soon as the tournament becomes active (e.g. the 2019 tournament's
               'Additional' puzzles).
        end: The datetime that submissions to the round close. Same format as `start`.
             If excluded puzzle is open until the tournament is ended (e.g. the 2019 tournament's 'Additional' puzzles).
        """
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata()

        # Check attached puzzle
        assert len(ctx.message.attachments) == 1, "Expected one attached puzzle file!"

        puzzle_file_name = ctx.message.attachments[0].filename
        if not puzzle_file_name.endswith('.puzzle'):
            # TODO: Could fall back to slugify(level.name) or slugify(round_name) for the .puzzle file name if the extension
            #       doesn't match
            raise ValueError("Attached file should use the extension .puzzle")

        level_bytes = await ctx.message.attachments[0].read()
        level_code = level_bytes.decode("utf-8")
        level = schem.Level(level_code)

        # Parse and check validity of start/end dates then rewrite them with UTC default ISO standard
        def parse_datetime_str(s):
            dt = datetime.fromisoformat(s)

            # If timezone unspecified, assume UTC, else convert to UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)

            return dt

        if start is not None:
            start_dt = parse_datetime_str(start)
        else:
            start_dt = datetime.now(timezone.utc)
        start = start_dt.isoformat()

        if end is not None:
            end_dt = parse_datetime_str(end)
            if end_dt <= start_dt:
                raise ValueError("Round end time is not after round start time.")
            elif end_dt <= datetime.now(timezone.utc):
                raise ValueError("Round end time is in past.")
            end = end_dt.isoformat()

        validate_metric(metric)

        # Check for directory conflict before doing any writes
        round_dir_name = f'{slugify(round_name)}_{slugify(level.name)}'
        round_dir = tournament_dir / round_dir_name
        if round_dir.exists():
            raise FileExistsError(f"Round directory {round_dir} already exists")

        if level.name in tournament_metadata['rounds']:
            raise ValueError(f"Puzzle with name `{level.name}` already exists in the current tournament")

        tournament_metadata['rounds'][level.name] = {'dir': round_dir_name,
                                                     'round_name': round_name,
                                                     'start': start,
                                                     'metric': metric, 'total_points': total_points}
        if end is not None:
            tournament_metadata['rounds'][level.name]['end'] = end

        with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as f:
            json.dump(tournament_metadata, f, ensure_ascii=False, indent=4)

        round_dir.mkdir()

        # Store the .puzzle file
        with open(round_dir / puzzle_file_name, 'w', encoding='utf-8') as f:
            f.write(level_code)

        (round_dir / 'solutions.txt').touch()

            # TODO: Track the history of each player's scores over time and do cool graphs of everyone's metrics going
            #       down as the deadline approaches!
            #       Can do like the average curve of everyone's scores over time and see how that curve varies by level
            #       Probably don't store every solution permanently to avoid the tournament.zip getting bloated but can
            #       at least keep the scores from replaced solutions.

            # TODO 2: Pareto frontier using the full submission history!

        await ctx.send(f"Successfully added {round_name} {level.name} to {tournament_metadata['name']}")

    # TODO: tournament-update-puzzle (e.g. puzzle change, extending deadline, changing flavour text typo, etc)
    #       should basically be same as add-puzzle but it can overwrite and maybe re-validates solutions.txt if the
    #       puzzle file changed

    # TODO: DDOS-mitigating measures such as:
    #       - maximum expected cycle count (set to e.g. 1 million unless specified otherwise for a puzzle) above which
    #         solutions are rejected and requested to be sent directly to the tournament host
    #       - limit user submissions to like 2 per minute

    # TODO: Accept blurb: https://discordpy.readthedocs.io/en/latest/ext/commands/commands.html#keyword-only-arguments
    @commands.command(name='tournament-submit')
    #@commands.dm_only()  # TODO: Give the bot permission to delete !tournament-submit messages from public channels since
                         #       someone will inevitably forget to use DMs
    async def tournament_submit(self, ctx):
        """Submit the attached solution file to the matching active tournament puzzle."""
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata()

        assert len(ctx.message.attachments) == 1, "Expected one attached solution file!"
        soln_bytes = await ctx.message.attachments[0].read()
        try:
            soln_str = soln_bytes.decode("utf-8")
        except UnicodeDecodeError as e:
            raise Exception("Attachment must be a plaintext file (containing a Community Edition export).") from e

        level_name, author, expected_score, soln_name = schem.Solution.parse_metadata(soln_str)
        soln_descr = schem.Solution.describe(level_name, author, expected_score, soln_name)

        # Discord tag used to ensure no name collision errors or exploits, and so the host knows who to message in case of
        # any issues
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
        elif ('end' not in round_metadata or msg_time > datetime.fromisoformat(round_metadata['end'])):
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
        # TODO: Provide seconds or minutes ETA based on estimate of 2,000,000 cycles / min
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

        reply = f"Successfully validated {soln_descr}, metric score: {soln_metric_score:.1g}"

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
            f.write('\n'.join(new_soln_strs))

        # TODO: Update submissions_history.txt with time, name, score, and blurb

        await ctx.message.add_reaction('✅')
        await msg.edit(content=reply)

    # TODO: @bot.command(name='tournament-name-change')

    @commands.command(name='tournament-info')
    #@commands.dm_only()  # Prevent public channel spam and make sure TO can't accidentally leak current round results
    async def tournament_info(self, ctx, puzzle_name=None):
        """List information on the active tournament or if provided, the specified puzzle."""
        # TODO: Accept round name as an alternative
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata()

        if puzzle_name is None:
            # List rounds
            embed = discord.Embed(
                title=tournament_metadata['name'],
                description="**Rounds**:")

            # TODO: Probably want to sort these by start or something
            for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                if 'start_post' in round_metadata:
                    embed.description += f"\n{round_metadata['round_name']}, {puzzle_name}: [Announcement]({round_metadata['start_post']})"
                    if 'end_post' in round_metadata:
                        embed.description += f" | [Results]({round_metadata['end_post']})"

            # TODO: Add current standings

            await ctx.send(embed=embed)
            return

        # Make sure we return the same error message whether the puzzle doesn't exist or user doesn't have permission to
        # see it, so future round names can't be inferred
        puzzle_nonexistent_or_not_closed_exc = FileNotFoundError(f"Puzzle `{puzzle_name}` has not ended or does not exist")
        if puzzle_name not in tournament_metadata['rounds']:
            raise puzzle_nonexistent_or_not_closed_exc

        round_metadata = tournament_metadata['rounds'][puzzle_name]

        if 'start_post' not in round_metadata:
            # TODO: Provide the TO with info similar to the upcoming announcement post so they can check for errors
            raise puzzle_nonexistent_or_not_closed_exc

        embed = discord.Embed(title=f"{round_metadata['round_name']}, {puzzle_name}",
                              description=f"[Announcement]({round_metadata['start_post']})")

        # Prevent non-TO users from accessing rounds that haven't ended or that the bot hasn't announced the results of yet
        if 'end_post' in round_metadata:
            embed.description += f" | [Results]({round_metadata['end_post']})"
        elif (await self.bot.is_owner(ctx.author)):
            # If this is the TO, calculate and append the current standings so they can keep an eye on its progress

            # TODO: Merge all the below code with the similar announce_tournament_results code
            round_dir = tournament_dir / round_metadata['dir']

            with open(round_dir / 'solutions.txt', 'r', encoding='utf-8') as sf:
                solns_str = sf.read()

            puzzle_file = next(round_dir.glob('*.puzzle'), None)
            if puzzle_file is None:
                print(f"Error: {round_dir} puzzle file not found!")
                raise FileNotFoundError(f"{round_dir} puzzle file not found; I seem to be experiencing an error.")
            with open(puzzle_file, 'r', encoding='utf-8') as pf:
                level_code = pf.read()

            with open(round_dir / 'solutions.txt', 'r', encoding='utf-8') as sf:
                solns_str = sf.read()

            _results_str = results_str(solns_str, level_code, round_metadata['metric'],
                                       puzzle_points=round_metadata['total_points'])

            await ctx.send(f"Results:\n```\n{_results_str}\n```", embed=embed)
            return

        await ctx.send(embed=embed)

    # TODO tournament-submit-non-scoring-solution

    def results_str(self, solns_str, level_code, metric, puzzle_points=None):
        """Given a solutions.txt, level, and metric, return a formatted string of the players' ranked results.
        Format: Rank,Player,Score,Metric Score
        If puzzle points is included add: ,Rel. Metric,Points
        """
        results = "# ,Player      ,Score          ,Metric Score"
        if puzzle_points is not None:
            results += ",Rel. Metric,Points"

        level = schem.Level(level_code)
        solutions = [schem.Solution(level, soln_str) for soln_str in schem.Solution.split_solutions(solns_str)]

        if not solutions:
            return results

        # Calculate each score and the top score
        metric_scores = [eval_metric(solution, metric) for solution in solutions]
        min_metric_score = min(metric_scores)

        # Sort by metric and add lines
        last_rank = 0  # For tie-handling
        last_metric = None
        for rank, (solution, metric_score) in enumerate(sorted(zip(solutions, metric_scores), key=lambda x: x[1])):
            results += (f"\n{str(rank).ljust(2)},{solution.author.ljust(12)},{str(solution.expected_score).ljust(15)}"
                        + f",{f'{metric_score:.1g}'.ljust(12)}")
            if puzzle_points is not None:
                relative_metric = min_metric_score / metric_score
                points = puzzle_points * relative_metric
                results += f",{f'{relative_metric:.3g}'.ljust(11)},{f'{points:.3g}'.ljust(6)}"

        return results

    @tasks.loop(minutes=5)
    async def announce_tournament_round_start(self):
        """Announce any rounds that just started."""
        if not ACTIVE_TOURNAMENT_FILE.exists():
            return

        await self.bot.wait_until_ready()  # Looks awkward but apparently get_channel can return None if bot isn't ready
        channel = self.bot.get_channel(ANNOUNCEMENTS_CHANNEL_ID)

        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata()

        # Announce any round that started in the last hour and hasn't already been anounced
        cur_time = datetime.now(timezone.utc)
        for puzzle_name, round in tournament_metadata['rounds'].items():
            # Ignore round if it was already announced or didn't open for submissions in the last hour.
            # The hour limit is to ensure the bot doesn't spam too many announcements if something goes nutty,
            # while leaving some flex time in case the bot was down for some reason right after the puzzle started.
            start_dt = datetime.fromisoformat(round['start'])
            if 'start_post' in round or not 0 <= (cur_time - start_dt).total_seconds() <= 3600:
                # If we missed the hour limit but the puzzle hasn't ended, log a warning
                if 'start_post' not in round and ('end' not in round or cur_time < datetime.fromisoformat(round['end'])):
                    # This will spam the log but only 96 times a day...
                    print(f"Error: Puzzle {puzzle_name} was not announced but should be open for submissions")
                continue

            print(f"Announcing {puzzle_name} start")

            round_dir = tournament_dir / round['dir']

            puzzle_file = next(round_dir.glob('*.puzzle'), None)
            if puzzle_file is None:
                raise FileNotFoundError(f"{round_dir} puzzle file not found")

            with open(puzzle_file, 'r', encoding='utf-8') as pf:
                level_code = pf.read()

            single_line_level_code = level_code.replace('\r', '').replace('\n', '')

            # Discord's embeds seem to be the only way to do a hyperlink to hide the giant puzzle preview link
            announcement = discord.Embed(
                #author=tournament_name # TODO
                title=f"Announcing round {round['round_name']}, {puzzle_name}!",
                #description=flavour_text, # TODO
            )
            announcement.add_field(name='Preview',
                                   value=f"[Coranac Viewer]({CORANAC_SITE}?code={single_line_level_code})",
                                   inline=True)
            announcement.add_field(name='Metric', value=round['metric'], inline=True)
            round_end = round['end'] + ' UTC' if 'end' in round else "Tournament Close"
            announcement.add_field(name='Deadline', value=round_end, inline=True)
            # TODO: Add @tournament or something that notifies people who opt-in, preferably updateable by bot

            # Call synchronously to ensure no other coroutine can read/write tournament data (else we might overwrite
            # them)
            # TODO: Might be worth using an async lock for the tournament metadata file so the below can be async with
            #       any bot coroutines that aren't accessing the tournament metadata (same for announce_results)
            msg = channel.send(embed=announcement, file=discord.File(str(puzzle_file), filename=puzzle_file.name))

            # Keep the link to the original announcement post for !tournament-info. We can also check this to know whether
            # we've already done an announcement post
            round['start_post'] = msg.jump_url

        with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as tm_f:
            json.dump(tournament_metadata, tm_f, ensure_ascii=False, indent=4)

    @tasks.loop(minutes=5)
    async def announce_tournament_round_results(self):
        """Announce the results of any rounds that ended 15+ minutes ago."""
        if not ACTIVE_TOURNAMENT_FILE.exists():
            return

        await self.bot.wait_until_ready()  # Looks awkward but apparently get_channel can return None if bot isn't ready
        channel = self.bot.get_channel(ANNOUNCEMENTS_CHANNEL_ID)

        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata()

        # Announce the end of any round that ended over 15 min to 1:15 min ago and hasn't already has its end ennounced
        # The 15 minute delay is to give time for any last-minute submissions to be validated
        # TODO: Use some aync locks or some other proper way to ensure all submissions are done processing, without
        #       needing a hard-coded delay
        # The hour limit is just to make sure the bot doesn't spam old announcements if something goes nutty
        cur_time = datetime.now(timezone.utc)
        for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
            # Ignore puzzles that don't end until the tournament is over
            if 'end' not in round_metadata:
                continue

            end_dt = datetime.fromisoformat(round_metadata['end'])
            seconds_since_end = (cur_time - end_dt).total_seconds()
            if 'end_post' in round_metadata or not 900 <= seconds_since_end <= 4500: # 15 min to 1 hour 15 min
                # If the hour announcement window has passed and the results post was never made, log a warning
                if 'end_post' not in round_metadata and (cur_time - end_dt).total_seconds() > 4500:
                    # This will spam the log but only 96 times a day... indefinitely
                    print(f"Error: Puzzle {puzzle_name} has ended but results were not announced right afterward")
                continue

            print(f"Announcing {puzzle_name} results")

            round_dir = tournament_dir / round_metadata['dir']

            with open(round_dir / 'solutions.txt', 'r', encoding='utf-8') as sf:
                solns_str = sf.read()

            puzzle_file = next(round_dir.glob('*.puzzle'), None)
            if puzzle_file is None:
                raise FileNotFoundError(f"{round_dir} puzzle file not found")
            with open(puzzle_file, encoding='utf-8') as pf:
                level_code = pf.read()

            # TODO Modify standings.csv

            solns_file = round_dir / 'solutions.txt'
            with open(solns_file, 'r', encoding='utf-8') as sf:
                solns_str = sf.read()
            _results_str = results_str(solns_str, level_code, round['metric'],
                                       puzzle_points=round['total_points'])

            # Embed doesn't seem to be wide enough for tables
            # announcement = discord.Embed(
            #     #author=tournament_name # TODO
            #     title=f"{round['round_name']} ({puzzle_name}) Results",
            #     description=f"```\n{_results_str}\n```")
            #await channel.send(embed=announcement)
            announcement = f"{round['round_name']} ({puzzle_name}) Results"
            announcement += f"\n```\n{_results_str}\n```"

            # TODO: Add current overall tournament standings

            # TODO: Also attach blurbs.txt

            # Call synchronously to ensure no other coroutine can read/write tournament data (else we might overwrite
            # them)
            msg = channel.send(announcement, file=discord.File(str(solns_file), filename=solns_file.name))

            round_metadata['end_post'] = msg.jump_url

        with open(tournament_dir / 'tournament_metadata.json', 'w', encoding='utf-8') as tm_f:
            json.dump(tournament_metadata, tm_f, ensure_ascii=False, indent=4)

bot.add_cog(Tournament(bot))

if __name__ == '__main__':
    bot.run(TOKEN)
