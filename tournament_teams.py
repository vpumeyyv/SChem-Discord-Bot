#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime, timezone
import json

import discord
from discord.ext import commands
import schem

from tournament_base import BaseTournament, is_tournament_host


class TournamentTeams(BaseTournament):
    """Class providing a tournament-info bot command."""

    is_host = commands.check(is_tournament_host)

    @is_host
    @commands.command(name='tournament-teams', aliases=['tt'])
    async def tournament_teams(self, ctx):
        """List all teams in the current tournament."""
        tournament_dir = self.get_active_tournament_dir_and_metadata(is_host=True)[0]
        with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
            participants = json.load(f)

        teams = {}
        for discord_tag, player_info in participants.items():
            if 'team' not in player_info:
                continue

            if player_info['team'] not in teams:
                teams[player_info['team']] = []

            # Display the player's nickname if available, else discord tag
            teams[player_info['team']].append(player_info['name'] if 'name' in player_info else discord_tag)

        if not teams:
            await ctx.send("No current tournament teams")
        else:
            await ctx.send("Current tournament teams:\n"
                           + "\n".join(f"  `{team_name}`: {', '.join(f'`{name}`' for name in player_names)}"
                                       for team_name, player_names in teams.items()))

    @is_host
    @commands.command(name='tournament-team-add', aliases=['tta', 'tournament-add-team', 'tat',
                                                           'tournament-team-create', 'tournament-create-team'])
    async def tournament_create_team(self, ctx, team_name, *players: discord.User):
        """Create a tournament team from the given discord users."""
        assert len(players) > 1, "Team must have at least 2 players!"

        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)
            with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
                participants = json.load(f)

            # First make sure this team name doesn't conflict with any player nicknames
            if any(team_name == player_info['name'] for player_info in participants.values()
                   if 'name' in player_info):
                raise ValueError(f"`{team_name}` is already in use as a player's nickname.")

            conflicting_teams = {}
            remove_submissions_by = set()  # Used for removing submissions to open rounds by any players whose team changed

            for player in players:
                discord_tag = str(player)
                if discord_tag not in participants:
                    # Allow adding a user with no nickname directly to a team
                    participants[discord_tag] = {'id': player.id, 'team': team_name}
                else:
                    if 'team' in participants[discord_tag] and participants[discord_tag]['team'] != team_name:
                        conflicting_teams[discord_tag] = participants[discord_tag]['team']
                        remove_submissions_by.add(participants[discord_tag]['team'])
                    elif 'team' not in participants[discord_tag]:
                        remove_submissions_by.add(participants[discord_tag]['name'])
                    # We can ignore the case where the player was already in this team

                    participants[discord_tag]['team'] = team_name

            # Ask for confirmation if any of the players is already in a team
            if conflicting_teams:
                confirm_msg = await ctx.send("The following player(s) are already in conflicting teams:\n"
                                             + "\n".join(f"{tag}: `{team}`" for tag, team in conflicting_teams.items())
                                             + "\nAre you sure you wish to change these players' teams?"
                                             + " React with ✅ within 30 seconds to proceed, ❌ to cancel all changes.")
                if not await self.wait_for_confirmation(ctx, confirm_msg):
                    return

            # Make sure we don't remove submissions by any team that wasn't fully disbanded
            # (TO should never partially change a team that has already submitted to an open round, but still)
            for player_info in participants.values():
                if 'team' in player_info and player_info['team'] in remove_submissions_by:
                    remove_submissions_by.remove(player_info['team'])

            # Search for and remove submissions to currently-open rounds by these players (or their old team if it no
            # longer exists)
            cur_time = datetime.now(timezone.utc).isoformat()
            for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                if 'start_post' in round_metadata and cur_time < round_metadata['end']:
                    with self.puzzle_submission_locks[puzzle_name]:
                        with open(tournament_dir / round_metadata['dir'] / 'solutions.txt', encoding='utf-8') as f:
                            solns_str = f.read()

                        new_soln_strs = []
                        for soln_str in schem.Solution.split_solutions(solns_str):
                            _, author, _, _ = schem.Solution.parse_metadata(soln_str)
                            if author not in remove_submissions_by:
                                new_soln_strs.append(soln_str)

                        with open(tournament_dir / round_metadata['dir'] / 'solutions.txt', 'w', encoding='utf-8') as f:
                            f.write('\n'.join(new_soln_strs))

                    # Fun submissions can be left alone as attribution is less important there and players can
                    # add/remove them as they see fit

            with open(tournament_dir / 'participants.json', 'w', encoding='utf-8') as f:
                json.dump(participants, f, ensure_ascii=False, indent=4)

        await ctx.send(f"Successfully created team {team_name} including {', '.join(str(player) for player in players)}")

    # @is_host
    # @commands.command(name='tournament-team-remove', aliases=['ttr', 'tournament-remove-team', 'trt'])
    # async def tournament_remove_team(self, ctx):
    #     pass
