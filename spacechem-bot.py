#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from discord.ext import commands
from dotenv import load_dotenv

from spacechem import game, solution

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

bot = commands.Bot(command_prefix='!',
                   description="SpaceChem-simulating bot developed in python by Zig."
                               + "\nRuns/validates Community-Edition-exported solution files, discluding legacy bugs.")

@bot.event
async def on_ready():
    print(f'{bot.user.name} has connected to Discord!')

@bot.command(name='run', aliases=['score', 'validate', 'check'],
             help='Run/validate the attached Community Edition solution file')
async def run(ctx):
    if len(ctx.message.attachments) != 1:
        await ctx.send(f"Expected one attached solution file!")
        return

    soln_bytes = await ctx.message.attachments[0].read()
    soln_str = soln_bytes.decode("utf-8")

    try:
        level_name, author, expected_score, soln_name = solution.Solution.parse_metadata_line(soln_str)
    except BaseException as e:
        await ctx.send(f"Error: Attached file does not appear to contain a SpaceChem CE export string")
        return

    soln_descr = f"[{level_name}] {expected_score}"
    if soln_name is not None:
        soln_descr += f' "{soln_name}"'
    soln_descr += f" by {author}"

    msg = await ctx.send(f"Running {soln_descr}, this should take < 30s barring an absurd cycle count...")

    try:
        score = game.run(soln_str)
    except BaseException as e:
        await msg.edit(content=f"Invalid solution:\n{e}")
        return

    if score == expected_score:
        await ctx.message.add_reaction('✅')
        await msg.edit(content=f"Successfully validated {soln_descr}")
    else:
        await msg.edit(content=f"Expected {expected_score} but got {score}")

# TODO:
# @bot.command(name='submit')
# async def submit(ctx):
#     # Sneakily react with a green check mark on msgs to the leaderboard-bot
#     # Auto-fetch pastebin link from youtube video description?

# @bot.command(name='start-tournament')
# @commands.is_owner()  # or @commands.has_role('tournament-host')
# async def start_tournament(ctx):
#     # probably store puzzles and submissions in a directory

# @bot.command(name='add-tournament-puzzle')
# @commands.is_owner()  # or @commands.has_role('tournament-host')
# async def add_tournament_puzzle(ctx)

# @tasks.loop(days=1)
# async def announce_tournament_puzzle_results():

# @tasks.loop(days=1)
# async def announce_tournament_results():

# @bot.command(name='random-level')

if __name__ == '__main__':
    bot.run(TOKEN)
