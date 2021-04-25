#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

import discord
from discord.ext import commands

from tournament_base import BaseTournament, is_tournament_host


class TournamentPlayers(BaseTournament):
    """Class providing player-related bot commands."""

    is_host = commands.check(is_tournament_host)

    @commands.command(name='tournament-who', aliases=['who', 'w'])
    async def who(self, ctx, *, nickname):
        """Return the discord user associated with the given nickname.

        nickname: The player's nickname (case-sensitive).
        """
        is_host = is_tournament_host(ctx)
        tournament_dir = self.get_active_tournament_dir_and_metadata(is_host=is_host)[0]

        with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
            participants = json.load(f)

        # TODO: Also accept team name

        for player_info in participants.values():
            if 'name' in player_info and player_info['name'] == nickname:
                await ctx.send(f"<@{player_info['id']}>", allowed_mentions=discord.AllowedMentions(users=False))
                return

        await ctx.send(f"No player with nickname `{nickname}`", allowed_mentions=discord.AllowedMentions(users=False))

    @commands.command(name='tournament-player-rename', aliases=['tpr'])
    @is_host
    async def set_player_name(self, ctx, player: discord.User, new_nickname):
        """Set the specified user's tournament nickname, updating submissions as needed.

        The player will also be DM'd to inform them of the change.
        """
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

        # TODO: If the rename occurs while a submission by this player is processing, we could get spooked if they
        #       do read name -> async validate -> write solution and we rename during the validation
        #       Really need to use proper reader/writer locks
        async with self.tournament_metadata_write_lock:
            with open(tournament_dir / 'participants.json', encoding='utf-8') as f:
                participants = json.load(f)

            tag = str(player)
            if tag not in participants:
                raise Exception(f"{player} not found in tournament participants")

            # Check for conflicts with an existing submit name (including their own)
            for player_info in participants.values():
                if 'name' in player_info and player_info['name'] == new_nickname:
                    await ctx.send(f"Error: `{new_nickname}` is already being used by <@{player_info['id']}>",
                                   allowed_mentions=discord.AllowedMentions(users=False))
                    return

            # Perform corresponding updates to open rounds if this is a rename
            old_nickname = participants[tag]['name'] if 'name' in participants[tag] else None
            if old_nickname is not None:
                for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                    # Ignore non-open rounds
                    # TODO: This makes the history somewhat confusing and prevents us from re-calculating the standings
                    #       changes caused by old rounds when we implement updating metametric.
                    #       To be able to update already-ended rounds too, we'll need to store old nicknames in them
                    #       or some such
                    if 'start_post' not in round_metadata or 'end_post' in round_metadata:
                        continue

                    round_dir = tournament_dir / round_metadata['dir']

                    # If they were submitting using their nickname, update the author on their submissions
                    if self.get_team_name(round_dir, tag) is None:
                        self.rename_submissions_by(round_dir, puzzle_name, old_nickname, new_nickname)

                    # Update the history file
                    self.rename_author_in_history(round_dir, puzzle_name, old_nickname, new_nickname)

            participants[tag]['name'] = new_nickname

            with open(tournament_dir / 'participants.json', 'w', encoding='utf-8') as f:
                json.dump(participants, f, ensure_ascii=False, indent=4)

        # DM the player to let them know of the new name
        previously = f" (previously `{old_nickname}`)" if old_nickname is not None else ""
        await player.send(f"The tournament host has set your nickname to `{new_nickname}`{previously}. Please ensure"
                          " this name appears in the first line of all your future submitted solution exports.")

        await ctx.send(f"Successfully set nickname of <@{player.id}> to `{new_nickname}`{previously} and informed them via DM.",
                       allowed_mentions=discord.AllowedMentions(users=False))
