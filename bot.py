#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import os

from discord.ext import commands
import schem

from tournament import Tournament

TOKEN = os.getenv('SCHEM_BOT_DISCORD_TOKEN')

bot = commands.Bot(command_prefix='!',
                   description="SpaceChem-simulating bot."
                               + "\nRuns/validates Community-Edition-exported solution files, excluding legacy bugs.")

@bot.event
async def on_ready():
    print(f'{bot.user.name} has connected to Discord!')

@bot.event
async def on_command_error(ctx, error):
    """Default bot command error handler."""
    if isinstance(error, (commands.CommandNotFound, commands.CheckFailure)):
        return  # Avoid logging errors when users put in invalid commands

    print(f"{type(error).__name__}: {error}")
    await ctx.send(str(error))  # Probably bad practice but it makes the commands' code nice...

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
