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
    @is_host
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
