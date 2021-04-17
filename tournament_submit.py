#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path

import discord
from discord.ext import commands
import schem

from metric import eval_metric
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
            msg_time = submission_msg.edited_at.replace(tzinfo=timezone.utc)
        else:
            msg_time = submission_msg.created_at.replace(tzinfo=timezone.utc)

        if msg_time < datetime.fromisoformat(round_metadata['start']):
            raise unknown_level_exc
        elif msg_time > datetime.fromisoformat(round_metadata['end']):
            raise Exception(f"Submissions for `{puzzle_name}` have closed.")

    @staticmethod
    def add_or_check_player(round_dir: Path, user: discord.User, nickname: str):
        """Given a discord user and nickname, register this participant or verify they match the originally-registered
        pairing. Return their team name if any or else None.
        """
        with open(round_dir.parent / 'participants.json', 'r', encoding='utf-8') as f:
            participants = json.load(f)
        with open(round_dir / 'teams.json', encoding='utf-8') as f:
            teams = json.load(f)

        discord_tag = str(user)

        # If the player is part of a team and submitted under their team name, just return their team name
        if nickname in teams and discord_tag in teams[nickname]:
            return nickname

        # If they aren't submitting under a team name, check if the nickname has already been set or must be set now
        if discord_tag in participants and 'name' in participants[discord_tag]:
            if nickname != participants[discord_tag]['name']:
                # TODO: Could allow name changes but it would be a lot of work and potentially confusing for the
                #       other participants, probably should only do this case-by-case and manually
                raise ValueError(f"Given author name `{nickname}` doesn't match your prior submissions':"
                                 + f" `{participants[discord_tag]['name']}`; please talk to the"
                                 + " tournament host if you would like a name change.")

            # If the above teams check didn't match and the nickname is registered, bad shit is happening
            assert nickname not in teams, "It looks like a team with your name exists, please yell at Zig"
        else:
            # Set nickname for the first time
            # TODO: Add teams listing to tournament metadata for checking for conflicts with other rounds' teams
            if nickname in teams or any('name' in d and nickname == d['name'] for d in participants.values()):
                raise PermissionError(f"Solution author name `{nickname}` is already in use by another player or team,"
                                      + " please choose another (or login to the correct discord account).")

            if discord_tag not in participants:
                participants[discord_tag] = {'id': user.id}

            # Separate from the above since if added by a team, a player may already exist but not have had a name
            participants[discord_tag]['name'] = nickname

            with open(round_dir.parent / 'participants.json', 'w', encoding='utf-8') as f:
                json.dump(participants, f, ensure_ascii=False, indent=4)

        # After adding nickname if possible, check if this user is in any team and return their team name if so
        for team_name, tags in teams.items():
            if discord_tag in tags:
                return team_name

        return None

    @commands.Cog.listener('on_message')
    async def tournament_submit_shortcut(self, msg):
        """Treat any non-command DM message containing an attachment(s) as a call to tournament-submit."""
        if msg.content.startswith(self.bot.command_prefix):
            await self.bot.process_commands(msg)  # Process normally
        elif msg.guild is None and msg.attachments:
            ctx = await self.bot.get_context(msg)
            # TODO: This is okay for now since our only pre-hook is a channel check, but if we ever need other hooks
            #       this should be called properly via process_commands (after prepending "!ts ")
            await self.tournament_submit(ctx, comment=msg.content)

    # TODO: Give the bot permission to delete !tournament-submit messages from public channels
    #       since someone will inevitably forget to use DMs
    @commands.command(name='tournament-submit', aliases=['ts'])
    @commands.dm_only()
    async def tournament_submit(self, ctx, *, comment=""):
        """Submit the attached solution file to the tournament.

        [Optional] comment: Comment to go with the solution. When the puzzle results
                            are announced, your submission history will be publicly
                            published along with any of these comments.
        [Attachment] *: A text file containing a single Community Edition-exported
                        solution string. To create this, open the solution then click
                        Export from the CE save menu (hamburger button below Undo).
        """
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata()

        assert len(ctx.message.attachments) == 1, "Expected one attached solution file!"
        soln_strs = await self.parse_solution_attachment(ctx.message.attachments[0], is_host=is_tournament_host(ctx))

        reaction = '✅'
        for soln_str in soln_strs:
            msg = None
            try:
                level_name, author, expected_score, soln_name = schem.Solution.parse_metadata(soln_str)

                # Check the round exists and the message is within its submission period
                self.verify_round_submission_time(ctx.message, tournament_metadata, level_name)

                round_metadata = tournament_metadata['rounds'][level_name]
                round_dir = tournament_dir / round_metadata['dir']

                # Skip participant name checks for the TO backdoor
                if not is_tournament_host(ctx):
                    team_name = self.add_or_check_player(round_dir, ctx.message.author, author)

                    # Change the author name if the submitter is part of a team
                    if team_name is not None:
                        author = team_name

                # Prefix the solution name with "[author] " for readability on import, and replace author if in a team
                new_soln_name = f"[{author}]" if soln_name is None else f"[{author}] {soln_name}"
                old_metadata_line = soln_str.strip().split('\n', maxsplit=1)[0]
                new_metadata_line = f"SOLUTION:{level_name},{author},{expected_score},{new_soln_name}"
                soln_str = soln_str.replace(old_metadata_line, new_metadata_line, 1)

                with self.puzzle_submission_locks[level_name]:
                    level = self.get_level(round_dir)

                    # Verify the solution
                    # TODO: Provide seconds or minutes ETA based on estimate of 2,000,000 cycles / min (/ reactor?)
                    soln_descr = schem.Solution.describe(level_name, author, expected_score, soln_name)
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

                    await msg.edit(content=f"Successfully validated {soln_descr}, metric score: {round(soln_metric_score, 3)}")

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
                                ctx.message.add_reaction('⚠')

                                confirm_msg = await ctx.send(
                                    "Warning: This solution regresses your last submission's metric score, previously: "
                                    + str(round(old_metric_score, 3)))
                                if not await self.wait_for_confirmation(ctx, confirm_msg):
                                    ctx.message.add_reaction('❌')
                                    return
                        else:
                            new_soln_strs.append(cur_soln_str)

                    new_soln_strs.append(soln_str)

                    with open(round_dir / 'solutions.txt', 'w', encoding='utf-8') as f:
                        # Make sure not to write windows newlines or python will double the carriage returns
                        f.write('\n'.join(new_soln_strs))

                    # Write the submission time, score, metric, and any comment to this author's submission history
                    with open(round_dir / 'submissions_history.json', 'r', encoding='utf-8') as f:
                        submissions_history = json.load(f)

                    if author not in submissions_history:
                        submissions_history[author] = []
                        # Sort by author name
                        submissions_history = {k: submissions_history[k] for k in sorted(submissions_history)}

                    submit_time = (ctx.message.created_at if ctx.message.edited_at is None
                                   else ctx.message.edited_at).replace(tzinfo=timezone.utc)

                    submissions_history[author].append((submit_time.isoformat(),
                                                        str(solution.expected_score),
                                                        soln_metric_score,
                                                        solution.name if solution.name else None,
                                                        comment if comment else None))

                    with open(round_dir / 'submissions_history.json', 'w', encoding='utf-8') as f:
                        json.dump(submissions_history, f, ensure_ascii=False, indent=4)
            except Exception as e:
                reaction = '❌'
                print(f"{type(e).__name__}: {e}")
                # Replace the 'Running...' message if it got that far
                if msg is not None:
                    await msg.edit(content=f"{type(e).__name__}: {e}")
                else:
                    await ctx.send(f"{type(e).__name__}: {e}")

        await ctx.message.add_reaction(reaction)

    # TODO: Limit each player to 5 non-scoring solutions
    # TODO: Give the bot permission to delete !tournament-submit messages from public channels
    #       since someone will inevitably forget to use DMs
    @commands.command(name='tournament-submit-fun', aliases=['tsf', 'tournament-submit-non-scoring', 'tsns'])
    @commands.dm_only()
    async def tournament_submit_fun(self, ctx):
        """Submit the attached *non-scoring* solution file to the tournament.

        Will replace any previous non-scoring solution of the same name.
        There are currently no limits on how many non-scoring solutions you may submit;
        please keep it reasonable (and use tournament-submission-fun-remove as needed).
        """
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata()

        assert len(ctx.message.attachments) == 1, "Expected one attached solution file!"
        soln_str = (await self.parse_solution_attachment(ctx.message.attachments[0]))[0]

        level_name, author, expected_score, soln_name = schem.Solution.parse_metadata(soln_str)
        soln_descr = schem.Solution.describe(level_name, author, expected_score, soln_name)

        # Check the round exists and the message is within its submission period
        self.verify_round_submission_time(ctx.message, tournament_metadata, level_name)

        round_metadata = tournament_metadata['rounds'][level_name]
        round_dir = tournament_dir / round_metadata['dir']

        # Register or verify this participant's nickname
        self.add_or_check_player(round_dir, ctx.message.author, author)

        # Prefix the solution name with "[author] " for readability on import
        soln_name = f"[{author}]" if soln_name is None else f"[{author}] {soln_name}"
        old_metadata_line = soln_str.strip().split('\n', maxsplit=1)[0]
        new_metadata_line = f"SOLUTION:{level_name},{author},{expected_score},{soln_name}"
        soln_str = soln_str.replace(old_metadata_line, new_metadata_line, 1)

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
            await loop.run_in_executor(None, solution.validate)

            reply = f"Added non-scoring submission {soln_descr}"

            # Add to solutions_fun.txt, replacing any existing solution by this author if it has the same solution name
            with open(round_dir / 'solutions_fun.txt', 'r', encoding='utf-8') as f:
                solns_str = f.read()

            new_soln_strs = []
            for cur_soln_str in schem.Solution.split_solutions(solns_str):
                _, cur_author, _, cur_soln_name = schem.Solution.parse_metadata(cur_soln_str)
                if cur_author == author and cur_soln_name == soln_name:
                    if soln_name == f"[{author}]":  # The default label only
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
    @commands.dm_only()
    async def tournament_list_submissions(self, ctx, *, round_or_puzzle_name=None):
        """List your puzzle submissions.

        If round/puzzle not specified, shows only currently-active puzzle submissions.

        round_or_puzzle_name: (Case-insensitive) If provided, show only your submissions
                              to the specified round/puzzle. May be a past puzzle.
                              A string like r10 will also match "Round 10" as a shortcut.
        """
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=False)
        nickname = self.get_player_name(tournament_dir, ctx.message.author)

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

            # Identify the name their submissions are stored under, accounting for this round's teams
            team_name = self.get_team_name(round_dir, ctx.message.author)
            submit_name = team_name if team_name is not None else nickname
            if submit_name is None:
                reply += f"\n{indent}No submissions."
                continue

            # Check for scoring solution
            with open(round_dir / 'solutions.txt', 'r', encoding='utf-8') as f:
                solns_str = f.read()

            has_scoring = False
            for soln_str in schem.Solution.split_solutions(solns_str):
                _, cur_author, score, soln_name = schem.Solution.parse_metadata(soln_str)
                if cur_author == submit_name:
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
                if cur_author == submit_name:
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

    @commands.command(name='tournament-submission-fun-remove', aliases=['tsfr',
                                                                        'tournament-submission-non-scoring-remove',
                                                                        'tournament-remove-fun-submission',
                                                                        'tournament-remove-non-scoring-submission'])
    @commands.dm_only()
    async def tournament_remove_fun_submission(self, ctx, round_or_puzzle_name, *, soln_name=None):
        """Remove a non-scoring submission to the given round/puzzle.

        round_or_puzzle_name: (Case-insensitive) The matching round/puzzle to remove a NS solution from.
                              Must be quoted if contains spaces.
                              A string like r10 will also match "Round 10" as a shortcut.
        soln_name: The name of the NS solution to remove (case-sensitive). If omitted, remove unnamed solution.
        """
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=False)
        puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name, is_host=False, missing_ok=False)
        round_metadata = tournament_metadata['rounds'][puzzle_name]
        round_dir = tournament_dir / round_metadata['dir']

        nickname = self.get_player_name(tournament_dir, ctx.message.author)
        team_name = self.get_team_name(round_dir, ctx.message.author)
        submit_name = team_name if team_name is not None else nickname
        if submit_name is None:
            raise Exception("You have no current submissions to this round.")

        # If the user passes no name or an empty string, interpret as an unnamed solution (which we store as "[author]")
        if not soln_name:
            soln_name = f"[{submit_name}]"
        elif not soln_name.startswith(f"[{submit_name}]"):  # No space so the error is legible if they messed up prefixing it themselves
            # Auto-prepend the author name we prefix onto submissions, if the user did not
            soln_name = f"[{submit_name}] " + soln_name

        # Prevent removal from a round whose results have already been published
        if 'end_post' in round_metadata:
            raise ValueError("Cannot remove submission from already-closed round!")

        with self.puzzle_submission_locks[puzzle_name]:
            with open(round_dir / 'solutions_fun.txt', 'r', encoding='utf-8') as f:
                solns_str = f.read()

            new_soln_strs = []
            reply = None
            for cur_soln_str in schem.Solution.split_solutions(solns_str):
                _, cur_author, score, cur_soln_name = schem.Solution.parse_metadata(cur_soln_str)
                if cur_author == submit_name and cur_soln_name == soln_name:
                    if reply is not None:
                        print(f"Internal Error: multiple non-scoring solutions to {puzzle_name} by {submit_name} have same name {soln_name}")

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
