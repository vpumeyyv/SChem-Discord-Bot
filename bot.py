#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import os

import discord
from discord.ext import commands
import schem

from tournament import Tournament

TOKEN = os.getenv('SCHEM_BOT_DISCORD_TOKEN')
MAINTAINER_DISCORD_ID = int(os.getenv('SCHEM_BOT_MAINTAINER_DISCORD_ID'))
ANNOUNCEMENTS_CHANNEL_ID = int(os.getenv('SCHEM_BOT_ANNOUNCEMENTS_CHANNEL_ID'))

bot = commands.Bot(command_prefix='!',
                   description="SpaceChem-simulating bot."
                               + "\nRuns/validates Community-Edition-exported solution files, excluding legacy bugs.")

@bot.before_invoke
async def is_valid_ctx(ctx):
    """Ignore commands not sent via DM or in the bot's designated channel."""
    if not (isinstance(ctx.channel, discord.channel.DMChannel) or ctx.channel.id == ANNOUNCEMENTS_CHANNEL_ID):
        raise commands.ChannelNotReadable(ctx.channel)

@bot.event
async def on_ready():
    print(f'{bot.user.name} has connected to Discord!')

@bot.event
async def on_command_error(ctx, error):
    """Default bot command error handler."""
    if isinstance(error, (commands.CommandNotFound, commands.CheckFailure, commands.ChannelNotReadable)):
        return  # Avoid logging errors when users put in invalid commands

    print(f"{type(error).__name__}: {error}")
    await ctx.send(str(error))  # Probably bad practice but it makes the commands' code nice...

@bot.command(name='about')
async def about(ctx):
    """Info about this bot."""
    await ctx.send(f"""Hi! I'm a bot for hosting the annual SpaceChem tournament.
To save weakling human tournament hosts from the effort of manually verifying every player submission, I accept Community Edition solution export files via private DM, and automatically validate them using a clean room implementation of the SpaceChem backend, which you can check out at <https://github.com/spacechem-community-developers/SChem>. This was created by Zig without access to the SC source code and has been tested reasonably thoroughly. It's expected that it's currently at full feature parity with SpaceChem, but 1 or 2 bugs may turn up during the tournament. If you find that it fails to correctly validate a solution that runs in SpaceChem, please DM <@{MAINTAINER_DISCORD_ID}>. If the bug can be demonstrated without spoiling your tournament solution to others, you can also directly open an issue in the SChem github project.

To see all available bot functionality, DM me `!help` or `!help <specific-command>` (to send a DM, right click my name and select 'Message'). The main commands of interest:
- !tournament-info - View info on the current tournament
- !tournament-submit - Submit an attached Community Edition solution file
- !tournament-submissions-list - View your submissions

Please note that I will only respond to submission-related commands if sent in a DM; do not attempt to submit a solution file in a public channel, for obvious reasons.

Finally, it hopefully goes without saying, but please do not deliberately DDOS me or submit an excessive number of non-scoring solutions. I'm just a small bot trying to make my way in the world with naught but a raspberry pi and limited SD card space!
""", allowed_mentions=discord.AllowedMentions(users=False))  # Don't actually ping me

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
    await loop.run_in_executor(None, schem.validate, soln_str)  # Default thread executor

    await ctx.message.add_reaction('✅')
    await msg.edit(content=f"Successfully validated {soln_descr}")

# TODO @bot.command(name='random-level')

# TODO: Ideally this and tournament_submit get merged
# @bot.command(name='submit')
# async def submit(ctx):
#     # Sneakily react with a green check mark on msgs to the leaderboard-bot?
#     # Auto-fetch pastebin link from youtube video description

bot.add_cog(Tournament(bot))

if __name__ == '__main__':
    bot.run(TOKEN)
