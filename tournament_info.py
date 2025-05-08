#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import io
import json

import discord
from discord.ext import commands

from tournament_base import BaseTournament, is_tournament_host
from utils import format_date, discord_date


class TournamentInfo(BaseTournament):
    """Class providing info-related bot commands."""

    is_host = commands.check(is_tournament_host)

    @commands.command(name='tournament-info', aliases=['ti'])
    async def tournament_info(self, ctx, *, round_or_puzzle_name=None):
        """Info on the tournament or specified round/puzzle.

        round_or_puzzle_name: (Case-insensitive) Return links to the matching
                              puzzle's announcement (/ results if available) posts, and video (if available).
                              A string like r10 will also match "Round 10" as a shortcut.
                              If not specified, show all puzzle announcement links
                              and current tournament standings.
        """
        is_host = is_tournament_host(ctx)
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=is_host)

        if round_or_puzzle_name is None:
            # Set up an embed listing all the rounds and their announcement messages
            embed = discord.Embed(title=tournament_metadata['name'], description="")

            if 'start_post' in tournament_metadata:
                embed.description += f"[Announcement]({tournament_metadata['start_post']})"

            embed.description += "\n**Rounds**:"
            for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                if 'start_post' in round_metadata:
                    embed.description += f"\n{round_metadata['round_name']}, {puzzle_name}:" \
                                         + f" [Announcement]({round_metadata['start_post']})"
                    if 'end_post' in round_metadata:
                        embed.description += f" | [Results]({round_metadata['end_post']})"
                        if 'video' in round_metadata:
                            embed.description += f" | [Video]({round_metadata['video']})"
                    else:
                        embed.description += f" {self.puzzle_deadline_str(round_metadata)}"
                elif is_host:
                    # Allow the TO to see schedule info on upcoming puzzles
                    embed.description += f"\n{round_metadata['round_name']}, {puzzle_name}:" \
                                         + f" Start: {discord_date(round_metadata['start'])}" \
                                         + f" | End: {discord_date(round_metadata['end'])}"

            await ctx.send(embed=embed)

            if 'start_post' in tournament_metadata:
                # Create a standings table (in chunks under discord's char limit as needed)
                for standings_msg in self.table_msgs(title_line="**Standings**",
                                                     table_text=self.standings_str(tournament_dir)):
                    await ctx.send(standings_msg)

            return

        # Convert to puzzle name (this also checks they have permission to view it)
        puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name, is_host=is_host, missing_ok=False)
        round_metadata = tournament_metadata['rounds'][puzzle_name]

        embed = discord.Embed(title=f"{round_metadata['round_name']}, {puzzle_name}")

        if 'start_post' in round_metadata:
            embed.description = f"[Announcement]({round_metadata['start_post']})"

            if 'end_post' in round_metadata:
                embed.description += f" | [Results]({round_metadata['end_post']})"
                if 'video' in round_metadata:
                            embed.description += f" | [Video]({round_metadata['video']})"
            else:
                embed.description += f" {self.puzzle_deadline_str(round_metadata)}"
        else:
            embed.description = f"Start: {discord_date(round_metadata['start'])}" \
                                + f" | End: {discord_date(round_metadata['end'])}"

        await ctx.send(embed=embed)

    @commands.command(name='tournament-submit-history', aliases=['tsh', 'sh', 'history'])
    @commands.dm_only()
    async def history(self, ctx, *, round_or_puzzle_name):
        """Show your history of submissions for the given round/puzzle.

        round_or_puzzle_name: (Case-insensitive) The round/puzzle name to get
                      your submissions history for.
                      A string like r10 will also match "Round 10" as a shortcut.
        """
        is_host = is_tournament_host(ctx)
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=False)
        puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name,
                                           is_host=is_host, missing_ok=False)
        round_metadata = tournament_metadata['rounds'][puzzle_name]
        round_dir = tournament_dir / round_metadata['dir']

        if not is_host:
            # Get all names the player might have submitted under
            tag = str(ctx.message.author)
            past_submit_names = []

            # Nickname
            with open(tournament_dir / 'participants.json', encoding='utf-8') as f:
                participants = json.load(f)
            if tag in participants and 'name' in participants[tag]:
                past_submit_names.append(participants[tag]['name'])

            # Team name
            team_name = self.get_team_name(round_dir, tag)
            if team_name is not None:
                past_submit_names.append(team_name)

            # Get the submission history as a text string
            submit_history_str = self.get_submit_history(round_dir, authors=past_submit_names)
        else:
            # If the TO is using this, show all submissions to the given puzzle, sorted by date
            # We also need to show them raw timestamps so they can give a sufficiently precise argument to
            # !tournament-submission-delete
            submit_history_str = self.get_submit_history(round_dir, sort_by_date=True, raw_timestamps=True)

        # Attach the submission history as a file
        if submit_history_str:
            with io.StringIO() as f:
                f.write(submit_history_str)
                f.seek(0)  # Reset file io position

                await ctx.send(file=discord.File(f, filename='submissions_history.txt'))
        else:
            await ctx.send(f"No past submissions to {round_metadata['round_name']}")

    @commands.command(name='tournament-preview', aliases=['tp', 'announcement-preview', 'ap'])
    @is_host
    async def announcement_preview(self, ctx, round_or_puzzle_name=None, no_graphs=None):
        """Preview the specified round/puzzle's announcement or results post, or the
        tournament announcement post if unspecified.

        round_or_puzzle_name: (Case-insensitive) The round to preview the next announcement for.
                              A string like r10 will also match "Round 10" as a shortcut.
                              If not specified, show the tournament start announcement.
        no_graphs: If provided (any string works, e.g. no_graphs), do not generate the .html graphs while
                   previewing a puzzle's results announcement. This saves about 2 seconds.
        """
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

        if round_or_puzzle_name is None:
            assert 'start_post' not in tournament_metadata, "Round/puzzle name required for previews during tournament."

            # Preview the tournament announcement post
            await ctx.send(f"On {discord_date(tournament_metadata['start'])} the following announcement will be sent:")
            for msg_string in self.tournament_announcement(tournament_dir, tournament_metadata):
                await ctx.send(msg_string)

            return

        # Convert to puzzle name
        puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name, is_host=True, missing_ok=False)
        round_metadata = tournament_metadata['rounds'][puzzle_name]

        if 'start_post' not in round_metadata:
            # Preview the puzzle start announcement post
            await ctx.send(f"On {discord_date(round_metadata['start'])} the following announcement will be sent:")
            embed, attachment = self.round_announcement(tournament_dir, tournament_metadata, puzzle_name)
            await ctx.send(embed=embed, file=attachment)
            return

        # Otherwise preview the puzzle results post
        assert 'end_post' not in round_metadata, f"{round_metadata['round_name']} is already closed; nothing to preview."

        await ctx.send(f"On {discord_date(round_metadata['end'])} (+ 5 min for banter) the following announcement"
                       " will be sent:")

        # Send each of the sub-2000 char announcement messages, adding the attachments to the last one
        msg_strings, attachments, _ = self.round_results_announcement_and_standings_change(
                                          tournament_dir, tournament_metadata, puzzle_name, include_graphs=(no_graphs is None))
        for i, msg_string in enumerate(msg_strings):
            if i < len(msg_strings) - 1:
                await ctx.send(msg_string)
            else:
                await ctx.send(msg_string, files=attachments)

    @commands.command(name='tournament-standings-preview', aliases=['tsp', 'tournament-preview-standings', 'tps'])
    @is_host
    # TODO: round arg, only show standings if the specified round were added to current
    async def standings_preview(self, ctx):
        """Preview the standings if all open rounds were tallied right now."""
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

        with open(tournament_dir / 'standings.json', 'r', encoding='utf-8') as f:
            standings = json.load(f)

        name_to_discord_tags = self.nickname_to_discord_tags_dict(tournament_dir)  # Reverse lookup dict needed for updates

        # Tally the current results of each open puzzle and add them to the current standings
        for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
            if 'start_post' in round_metadata and 'end_post' not in round_metadata:
                standings_delta = self.round_results_announcement_and_standings_change(tournament_dir,
                                                                                       tournament_metadata,
                                                                                       puzzle_name)[2]

                # Create a teams-aware version of the lookup dict based on the teams for this puzzle
                with open(tournament_dir / round_metadata['dir'] / 'teams.json') as f:
                    teams = json.load(f)
                round_name_to_tags_dict = copy.deepcopy(name_to_discord_tags)
                round_name_to_tags_dict.update(teams)

                self.update_standings_dict(standings, standings_delta, round_name_to_tags_dict)

        # Create a standings table (in chunks under discord's char limit as needed)
        for standings_msg in self.table_msgs(title_line="**Standings**",
                                             table_text=self.standings_dict_to_str(tournament_dir, standings)):
            await ctx.send(standings_msg)
