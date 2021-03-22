#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path

import discord
from discord.ext import commands
import schem

from metric import eval_metric, format_metric
from tournament_base import BaseTournament, is_tournament_host


class TournamentSubmit(BaseTournament):
    """Submission-related tournament commands and utils."""

    @staticmethod
    async def parse_solution_attachment(attachment: discord.Attachment, is_host=False):
        """Given a discord Attachment expected to be a solution file, return a list of its solutions.
        If is_host is False, assert that only one solution is included.
        """
        soln_bytes = await attachment.read()
        try:
            soln_str = soln_bytes.decode("utf-8").replace('\r\n', '\n')
        except UnicodeDecodeError as e:
            raise Exception("Attachment must be a plaintext file (containing a Community Edition export).") from e

        soln_strs = list(schem.Solution.split_solutions(soln_str))
        assert len(soln_strs) != 0, "Attachment does not contain SpaceChem solution string(s)"
        assert len(soln_strs) == 1 or is_host, "Expected only one solution in the attached file."

        return soln_strs

    @staticmethod
    def verify_round_submission_time(submission_msg: discord.Message, tournament_metadata: dict, puzzle_name: str):
        """Raise an appropriate error if the given puzzle does not exist or the given message was not sent/edited
        during its submission time.
        """
        # Since otherwise future puzzle names could in theory be searched for, make sure we return the same message
        # whether a puzzle does not exist or is not yet open for submissions
        unknown_level_exc = ValueError(f"No active tournament level `{puzzle_name}`; ensure the first line of"
                                       + " your solution export has the correct level name.")
        if puzzle_name not in tournament_metadata['rounds']:
            raise unknown_level_exc
        round_metadata = tournament_metadata['rounds'][puzzle_name]

        if submission_msg.edited_at is not None:
            # Cover any possible late-submission exploits if we ever allow bot to respond to edits
            msg_time = submission_msg.edit_at.replace(tzinfo=timezone.utc)
        else:
            msg_time = submission_msg.created_at.replace(tzinfo=timezone.utc)

        if msg_time < datetime.fromisoformat(round_metadata['start']):
            raise unknown_level_exc
        elif msg_time > datetime.fromisoformat(round_metadata['end']):
            raise Exception(f"Submissions for `{puzzle_name}` have closed.")

    @staticmethod
    def add_or_check_player(tournament_dir: Path, user: discord.User, nickname: str):
        """Given a discord user and nickname, register this participant or verify they match the originally-registered
        pairing.
        """
        # Register this discord_id: [discord_tag, author_name] mapping if it is not already registered
        # If the solution's author_name conflicts with that of another player, request they change it
        with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
            participants = json.load(f)

        discord_tag = str(user)
        if discord_tag in participants:
            if nickname != participants[discord_tag][1]:
                # TODO: Could allow name changes but it would be a lot of work and potentially confusing for the
                #       other participants, probably should only do this case-by-case and manually
                raise ValueError(f"Given author name `{nickname}` doesn't match your prior submissions':"
                                 + f" `{participants[discord_tag][1]}`; please talk to the"
                                 + " tournament host if you would like a name change.")
        else:
            # First submission
            if nickname in (name for _, name in participants.values()):
                raise PermissionError(f"Solution author name `{nickname}` is already in use by another participant,"
                                      + " please choose another (or login to the correct discord account).")
            # Store both user tag and ID since former is TO-readable and latter is API-usable
            participants[discord_tag] = [user.id, nickname]

        with open(tournament_dir / 'participants.json', 'w', encoding='utf-8') as f:
            json.dump(participants, f, ensure_ascii=False, indent=4)

    # TODO: Accept blurb: https://discordpy.readthedocs.io/en/latest/ext/commands/commands.html#keyword-only-arguments
    @commands.command(name='tournament-submit', aliases=['ts'])
    #@commands.dm_only()  # TODO: Give the bot permission to delete !tournament-submit messages from public channels
    #       since someone will inevitably forget to use DMs
    async def tournament_submit(self, ctx):
        """Submit the attached solution file to the tournament."""
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata()

        assert len(ctx.message.attachments) == 1, "Expected one attached solution file!"
        soln_strs = await self.parse_solution_attachment(ctx.message.attachments[0], is_host=is_tournament_host(ctx))

        reaction = '✅'
        for soln_str in soln_strs:
            msg = None
            try:
                level_name, author, expected_score, soln_name = schem.Solution.parse_metadata(soln_str)
                soln_descr = schem.Solution.describe(level_name, author, expected_score, soln_name)

                # Skip participant name checks for the TO backdoor
                if not is_tournament_host(ctx):
                    self.add_or_check_player(tournament_dir, ctx.message.author, author)

                # Check the round exists and the message is within its submission period
                self.verify_round_submission_time(ctx.message, tournament_metadata, level_name)

                round_metadata = tournament_metadata['rounds'][level_name]
                round_dir = tournament_dir / round_metadata['dir']

                with self.puzzle_submission_locks[level_name]:
                    level = self.get_level(round_dir)

                    # Verify the solution
                    # TODO: Provide seconds or minutes ETA based on estimate of 2,000,000 cycles / min (/ reactor?)
                    msg = await ctx.send(f"Running {soln_descr}, this should take < 30s barring an absurd cycle count...")

                    solution = schem.Solution(level, soln_str)

                    # Call the SChem validator in a thread so the bot isn't blocked
                    # TODO: Provide max_cycles arg based on the round
                    # TODO: ProcessPoolExecutor might be more appropriate instead of the default (thread pool), but not
                    #  sure if the overhead for many small submissions is going to add up more than with threads and/or
                    #  if limitations on number of processes is the bigger factor
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, solution.validate)  # Default thread pool executor

                    # TODO: if metric uses 'outputs' as a var, we should instead catch any run errors (or just
                    #       PauseException, to taste) and pass the post-run solution object to eval_metric regardless

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
        soln_str = (await self.parse_solution_attachment(ctx.message.attachments[0]))[0]

        level_name, author, expected_score, soln_name = schem.Solution.parse_metadata(soln_str)
        soln_descr = schem.Solution.describe(level_name, author, expected_score, soln_name)

        # Register or verify this participant's nickname
        self.add_or_check_player(tournament_dir, ctx.message.author, author)

        # Check the round exists and the message is within its submission period
        self.verify_round_submission_time(ctx.message, tournament_metadata, level_name)

        round_metadata = tournament_metadata['rounds'][level_name]
        round_dir = tournament_dir / round_metadata['dir']

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
            await loop.run_in_executor(self.thread_pool_executor, solution.validate)

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

    @commands.command(name='tournament-submissions-list', aliases=['tsl',
                                                                   'tournament-submissions',
                                                                   'tournament-list-submissions', 'tls'])
    #@commands.dm_only()  # Prevent public channel spam and make sure TO can't accidentally leak current round results
    async def tournament_list_submissions(self, ctx, *, round_or_puzzle_name=None):
        """List your puzzle submissions.

        If round/puzzle not specified, shows only currently-active puzzle submissions.

        round_or_puzzle_name: (Case-insensitive) If provided, show only your submissions
                              to the specified round/puzzle. May be a past puzzle.
        """
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=False)
        player_name = self.get_player_name(tournament_dir, ctx.message.author, missing_ok=True)
        if player_name is None:
            await ctx.send("You have no current tournament submissions.")
            return

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

    # TODO: DDOS-mitigating measures such as:
    #       - maximum expected cycle count (set to e.g. 1 million unless specified otherwise for a puzzle) above which
    #         solutions are rejected and requested to be sent directly to the tournament host
    #       - limit user submissions to like 2 per minute

    # TODO tournament-name-change
