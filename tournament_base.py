#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from datetime import datetime, timedelta
import json
import os
from pathlib import Path

import discord
from discord.ext import commands
from dotenv import load_dotenv
import schem

from metric import get_metric_and_terms, eval_metametric, get_metametric_term_values
from utils import split_by_char_limit, format_date, wait_until

load_dotenv()

GUILD_ID = int(os.getenv('SCHEM_BOT_GUILD_ID'))
ANNOUNCEMENTS_CHANNEL_ID = int(os.getenv('SCHEM_BOT_ANNOUNCEMENTS_CHANNEL_ID'))
CORANAC_SITE = "https://www.coranac.com/spacechem/mission-viewer"


# Used in a decorator for checking if a user has tournament-hosting permissions.
# Unfortunately can't be part of the Tournament cog since command.check doesn't pass self/cls
def is_tournament_host(ctx):
    """Check whether the given user has tournament-hosting permissions."""
    hosts_json_file = BaseTournament.TOURNAMENTS_DIR / 'hosts.json'
    if not hosts_json_file.exists():
        return False

    with open(hosts_json_file, encoding='utf-8') as f:
        return str(ctx.message.author) in json.load(f)['hosts']


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


class BaseTournament(commands.Cog):
    """Class defining base class variables (e.g. puzzle locks) and utils used by all types of tournament commands."""

    # Tournaments structure:
    # tournaments/
    #     hosts.json -> list of discord users with admin access to tournament commands
    #     active_tournament.txt -> "slugified_tournament_name_1"
    #     slugified_tournament_name_1/
    #         tournament_metadata.json -> name, host, etc, + round dirs / metadata
    #         participants.json        -> discord_tag: discord_id, nickname, team_name (if any)
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

    @staticmethod
    def get_player_name(tournament_dir, discord_user: discord.User, missing_ok=True):
        """Given a discord user, get their nickname, or else team name if they are part of a team."""
        with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
            participants = json.load(f)

        discord_tag = str(discord_user)
        if discord_tag not in participants:
            if missing_ok:
                return None
            else:
                raise Exception("You have no current tournament submissions.")

        if 'team' in participants[discord_tag]:
            return participants[discord_tag]['team']
        else:
            assert 'name' in participants[discord_tag], \
                "Internal Error: Missing nickname or team name in participant info"
            return participants[discord_tag]['name']

    @staticmethod
    def get_puzzle_name(tournament_metadata, round_or_puzzle_name, is_host=False, missing_ok=True):
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

    @staticmethod
    def get_level(round_dir):
        """Given a round directory, return an schem.Level object based on its .puzzle file."""
        puzzle_file = next(round_dir.glob('*.puzzle'), None)
        if puzzle_file is None:
            print(f"Error: {round_dir} puzzle file not found!")
            raise FileNotFoundError("Round puzzle file not found; I seem to be experiencing an error.")

        with open(puzzle_file, 'r', encoding='utf-8') as f:
            level_code = f.read().strip()

        return schem.Level(level_code)

    @staticmethod
    def sorted_and_ranked(rows, sort_idx=-1, desc=False):
        """Given an iterable of rows containing strings or numeric types, return a list of them sorted on the given
        numeric column and with a rank prepended to each row.
        """
        last_score = None
        ranked_rows = []
        for i, row in enumerate(sorted(rows, key=lambda r: r[sort_idx], reverse=desc)):
            # Only increment rank if we didn't tie the previous value
            if row[sort_idx] != last_score:
                rank = i + 1
                last_score = row[sort_idx]

            ranked_rows.append([rank] + list(row))

        return ranked_rows

    @staticmethod
    def table_str(headers, rows, max_col_width=15):
        """Given an iterable of column headers and list of rows containing strings or numeric types, return a
        pretty-print string table with appropriate column widths.
        """
        # Prepend the header row and convert all given values to formatted strings
        formatted_rows = [headers] + [[x if isinstance(x, str) else str(round(x, 3)) for x in row]
                                      for row in rows]

        # truncate values over max col width
        for row in formatted_rows:
            for i in range(len(row)):
                if len(row[i]) > max_col_width:
                    row[i] = row[i][:max_col_width - 1] + '…'  # The ellipses unicode char

        # Get the minimum width of each column
        min_widths = [max(map(len, col)) for col in zip(*formatted_rows)]  # Sorry future reader

        return '\n'.join('  '.join(s.ljust(min_widths[i]) for i, s in enumerate(row)) for row in formatted_rows)

    @classmethod
    def standings_str(cls, tournament_dir):
        """Given a tournament's directory, return a string of the tournament standings"""
        with open(tournament_dir / 'standings.json', 'r', encoding='utf-8') as f:
            standings = json.load(f)
        table = [[k, v] for k, v in standings['total'].items()]

        # Display each participant by nickname if possible, falling back to discord_tag if not
        # Awkward that it's stored only by discord tag, but nickname is not guaranteed to exist in the case of a team,
        # and storing a mix runs into issues with keeping them unique - if someone sets their nickname to someone else's
        # discord tag before that person has joined as a participant, we'd be in a mess.
        with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
            participants = json.load(f)
        for row in table:
            discord_tag = row[0]
            if discord_tag in participants and 'name' in participants[discord_tag]:
                row[0] = participants[discord_tag]['name']

        return cls.table_str(('#', 'Name', 'Score'), cls.sorted_and_ranked(table, desc=True))

    @staticmethod
    def tournament_announcement(tournament_metadata):
        """Return the tournament announcement text."""
        announcement = f"**Announcing the {tournament_metadata['name']}**"
        announcement += f"\nMetametric: `{tournament_metadata['metametric']}`"
        announcement += f"\nEnd date: {format_date(tournament_metadata['end'])}"

        return announcement

    @staticmethod
    def table_msgs(title_line, table_text):
        """Given the text for an announcement table and its title, format the table with its title line and return them
        in chunks that fit inside discord's 2000-char message limit.
        """
        # Split the table up so it will just fit under discord's 2000-char msg limit even with the title prepended
        table_chunks = split_by_char_limit(table_text, 1999 - len(title_line) - 9)  # -9 for newlines/backticks
        table_msgs = [f"```\n{s}\n```" for s in table_chunks]
        table_msgs[0] = title_line + '\n' + table_msgs[0]

        return table_msgs

    async def announce_tournament_start(self, tournament_metadata):
        """Wait until the tournament start date and then announce it."""
        # TODO: Try/except/print in these background announcement tasks is ugly af, find a better way
        try:
            assert 'start_post' not in tournament_metadata, "Tournament has already been announced!"

            # Wait until the tournament start time
            start = tournament_metadata['start']
            await wait_until(datetime.fromisoformat(start))

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

    @staticmethod
    def round_announcement(tournament_dir, tournament_metadata, puzzle_name,
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
        round_end = format_date(round_metadata['end'])
        if round_metadata['end'] == tournament_metadata['end']:
            round_end += " (Tournament Close)"
        embed.add_field(name='Deadline', value=round_end, inline=True)

        # TODO: Add @tournament or something that notifies people who opt-in, preferably updateable by bot

        return embed, attachment

    async def announce_round_start(self, puzzle_name, round_metadata):
        """Wait until the round start date and then announce it."""
        try:
            assert 'start_post' not in round_metadata, "Round has already been announced!"

            # Wait until the round start time
            start = round_metadata['start']
            await wait_until(datetime.fromisoformat(start))

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

    def round_results_announcement_and_standings_change(self, tournament_dir, tournament_metadata, puzzle_name):
        """Given tournament dir/metadata and a specified puzzle, return a list of strings of the announcement message(s)
        text for the puzzle results, a list of attachments, and a dict indicating the point changes by player or team.
        """
        round_metadata = tournament_metadata['rounds'][puzzle_name]
        round_dir = tournament_dir / round_metadata['dir']

        attachments = []

        level = self.get_level(round_dir)

        solns_file = round_dir / 'solutions.txt'
        with open(solns_file, 'r', encoding='utf-8') as sf:
            solns_str = sf.read()
        attachments.append(discord.File(str(solns_file), filename=solns_file.name))

        solutions = [schem.Solution(level, soln_str) for soln_str in schem.Solution.split_solutions(solns_str)]

        # Calculate each score and the top score
        metric_scores_and_terms = [get_metric_and_terms(solution, round_metadata['metric']) for solution in solutions]
        min_metric_score = min(x[0] for x in metric_scores_and_terms) if metric_scores_and_terms else None

        # Sort and rank the solutions by metric, and convert them to table rows.
        # Also calculate their metametric score here so we can normalize and calculate points after
        col_headers = []
        results = []
        metametrics = []
        for rank, solution, metric_score, term_values \
                in self.sorted_and_ranked([[s, m, tv] for s, (m, tv) in zip(solutions, metric_scores_and_terms)],
                                          sort_idx=1):
            # We'll add a standard-format score column directly and only add extra columns for non-standard metric terms
            for term_key in ('cycles', 'reactors', 'symbols'):
                if term_key in term_values:
                    del term_values[term_key]

            if not col_headers:
                col_headers = ['#', 'Name', 'Score'] + list(term_values.keys()) + ['Metric']

            row = [rank, solution.author, str(solution.expected_score)] + list(term_values.values()) + [metric_score]

            # Calculate metametric
            metametric_vars = {'your_metric': metric_score, 'best_metric': min_metric_score,
                               'your_rank_idx': rank - 1, 'num_solvers': len(solutions)}
            metametric = eval_metametric(tournament_metadata['metametric'], metametric_vars)
            metametrics.append(metametric)

            # Add columns for the relative metric and placement bonus if present in the metametric
            for term_name, term_val in zip(('Rel. Metric', 'Rank Bonus'),
                                           get_metametric_term_values(tournament_metadata['metametric'],
                                                                      metametric_vars)):
                if term_val is not None:
                    row.append(term_val)

                    # Add to header if not yet done
                    if len(col_headers) < len(row):
                        col_headers.append(term_name)

            results.append(row)

        # Normalize the metametric scores and award points
        col_headers.append('Points')
        max_metametric = metametrics[0]  # Since we already sorted
        standings_scores = {}  # player_name: points_earned
        for i, metametric in enumerate(metametrics):
            author = results[i][1]
            assert author not in standings_scores, "solutions.txt unexpectedly contains duplicate player"

            points = round_metadata['points'] * (metametric / max_metametric)
            results[i].append(points)
            standings_scores[author] = points

        # TODO: Shouldn't need a solution to parse the header row; extract these from the metric
        if not solutions:
            col_headers = ('#', 'Player', 'Score', 'Metric', 'Rel. Metric', 'Points')

        # Create messages for the scoring solutions table. Embed not used as it is not wide enough for tables
        msg_strings = self.table_msgs(title_line=f"**{round_metadata['round_name']} ({puzzle_name}) Results**",
                                      table_text=self.table_str(col_headers, results))

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

            msg_strings.extend(self.table_msgs(title_line="**Non-Scoring Submissions**",
                                               table_text=self.table_str(fun_col_headers, fun_table_rows)))

            attachments.append(discord.File(str(fun_solns_file), filename=fun_solns_file.name))

        return msg_strings, attachments, standings_scores

    @staticmethod
    def update_standings(tournament_dir, puzzle_name, standings_delta):
        """Given a dict of player/team names to points delta, update the tournament standings."""
        with open(tournament_dir / 'standings.json', 'r', encoding='utf-8') as f:
            standings = json.load(f)

        standings['rounds'][puzzle_name] = standings_delta

        # Create a reverse nickname_or_team_name : discord_tag dict for ease of lookup
        with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
            participants = json.load(f)
        name_to_discord_tags = {}
        for discord_tag, player_info in participants.items():
            if 'team' in player_info:
                if player_info['team'] not in name_to_discord_tags:
                    name_to_discord_tags[player_info['team']] = []
                name_to_discord_tags[player_info['team']].append(discord_tag)
            else:
                name_to_discord_tags[player_info['name']] = [discord_tag]

        # Add to the standings, ignoring 0 scores
        for name, points in standings_delta.items():
            if points != 0:
                # Handle the case where a player's submission was submitted by the TO backdoor and they have no
                # participant info. Name collision-avoidance is not guaranteed in this case
                if name not in name_to_discord_tags:
                    if name not in standings['total']:
                        standings['total'][name] = 0
                    standings['total'][name] += points

                    continue

                for discord_tag in name_to_discord_tags[name]:
                    if discord_tag not in standings['total']:
                        standings['total'][discord_tag] = 0
                    standings['total'][discord_tag] += points

        with open(tournament_dir / 'standings.json', 'w', encoding='utf-8') as f:
            json.dump(standings, f, ensure_ascii=False, indent=4)

    async def announce_round_results(self, puzzle_name, round_metadata):
        """Wait until the round end date and then announce its results."""
        try:
            assert 'end_post' not in round_metadata, "Round results have already been announced!"

            # Wait until the round start time + 5 seconds to ensure last-second submitters have grabbed the submission lock
            end = round_metadata['end']
            await wait_until(datetime.fromisoformat(end) + timedelta(seconds=5))

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
                self.update_standings(tournament_dir, puzzle_name, standings_delta)

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
            await wait_until(datetime.fromisoformat(end))

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
                msg_strings = self.table_msgs(title_line=f"**{tournament_metadata['name']} Results**",
                                              table_text=self.standings_str(tournament_dir))

                # Send each of the sub-2000 char announcement messages
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
