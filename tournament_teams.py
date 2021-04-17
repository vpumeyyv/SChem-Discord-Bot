#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime, timezone
import json
from pathlib import Path

import discord
from discord.ext import commands
import schem

from tournament_base import BaseTournament, is_tournament_host


class TournamentTeams(BaseTournament):
    """Class providing teams-related bot commands."""

    is_host = commands.check(is_tournament_host)

    @commands.command(name='tournament-teams', aliases=['tt'])
    async def tournament_teams(self, ctx, *, round_or_puzzle_name):
        """List all teams formed for the specified puzzle or round name.

        Note that this does not actually ping the mentioned users.

        round_or_puzzle_name: (Case-insensitive) Return teams in the matching round/puzzle.
                              A string like r10 will also match "Round 10" as a shortcut.
        """
        is_host = is_tournament_host(ctx)
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=is_host)
        puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name, is_host=is_host, missing_ok=False)
        round_metadata = tournament_metadata['rounds'][puzzle_name]
        round_dir = tournament_dir / round_metadata['dir']

        with open(tournament_dir / 'participants.json', encoding='utf-8') as f:
            participants = json.load(f)
        with open(round_dir / 'teams.json', encoding='utf-8') as f:
            teams = json.load(f)

        if not teams:
            await ctx.send(f"No teams in {round_metadata['round_name']}.")
        else:
            await ctx.send(
                f"{round_metadata['round_name']} teams:\n"
                + "\n".join(f"  `{team_name}`: "
                            + ', '.join(f"<@{participants[tag]['id']}> ("
                                        + (f"`{participants[tag]['name']}`" if 'name' in participants[tag] else tag)
                                        + ')'
                                        for tag in tags)
                            for team_name, tags in teams.items()),
                allowed_mentions=discord.AllowedMentions(users=False))

    def remove_submissions_by(self, round_dir: Path, puzzle_name: str, authors: set):
        """Remove all submissions from the given round that match any of the given authors."""
        with self.puzzle_submission_locks[puzzle_name]:
            for solns_file in (round_dir / 'solutions.txt', round_dir / 'solutions_fun.txt'):
                with open(solns_file, encoding='utf-8') as f:
                    solns_str = f.read()

                new_soln_strs = []
                for soln_str in schem.Solution.split_solutions(solns_str):
                    _, author, _, _ = schem.Solution.parse_metadata(soln_str)
                    if author not in authors:
                        new_soln_strs.append(soln_str)

                with open(solns_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(new_soln_strs))

    @commands.command(name='tournament-team-add', aliases=['tta', 'tournament-add-team', 'tat',
                                                           'tournament-team-create', 'tournament-create-team'])
    @is_host
    async def tournament_create_team(self, ctx, team_name, from_round, *players: discord.User):
        """Create a tournament team from the given discord users.

        If the team name already exists, members will be added or removed to match the given new list.

        team_name: The name of the team.
        from_round: The name of a puzzle/round. The selected players will be put in a
                    team for all rounds starting from the given round's start date.
        players: The discord users to include in the given team. If they were already
                 in a team for a currently-open round, confirmation will be asked.
        """
        assert len(players) > 1, "Team must have at least 2 players!"

        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            from_puzzle = self.get_puzzle_name(tournament_metadata, from_round, is_host=True, missing_ok=False)
            from_date = tournament_metadata['rounds'][from_puzzle]['start']

            with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
                participants = json.load(f)

            # First make sure this team name doesn't conflict with any player nicknames
            if any(team_name == player_info['name'] for player_info in participants.values()
                   if 'name' in player_info):
                raise ValueError(f"`{team_name}` is already in use as a player's nickname.")

            # Add each player as a participant if they were not already
            for player in players:
                discord_tag = str(player)
                if discord_tag not in participants:
                    participants[discord_tag] = {'id': player.id}

            with open(tournament_dir / 'participants.json', 'w', encoding='utf-8') as f:
                json.dump(participants, f, ensure_ascii=False, indent=4)

            # Update each relevant round
            updated_rounds = []
            skipped = False
            for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                # Ignore prior rounds and closed rounds
                if round_metadata['start'] < from_date or 'end_post' in round_metadata:
                    continue

                round_dir = tournament_dir / round_metadata['dir']

                with open(round_dir / 'teams.json', encoding='utf-8') as f:
                    teams = json.load(f)

                submit_names_to_remove = set()  # Nicknames or team names of players being put in the new team

                # Check players' prior teams and get names of players/teams whose submissions will need to be removed
                # Construct a reverse discord_tag-to-team dict to facilitate this
                tag_to_team = {discord_tag: team_name
                               for team_name, discord_tags in teams.items()
                               for discord_tag in discord_tags}
                old_teams = {}
                for player in players:
                    discord_tag = str(player)
                    old_teams[discord_tag] = tag_to_team[discord_tag] if discord_tag in tag_to_team else None

                    if discord_tag not in tag_to_team:
                        # If this participant had no prior team, just remove their submissions if any
                        if 'name' in participants[discord_tag]:
                            submit_names_to_remove.add(participants[discord_tag]['name'])
                    elif tag_to_team[discord_tag] != team_name:
                        other_team_name = tag_to_team[discord_tag]
                        teams[other_team_name].remove(discord_tag)  # Remove them from their old team

                        # If this was the last member of the other team, remove the other team's submissions
                        if not teams[other_team_name]:
                            submit_names_to_remove.add(other_team_name)

                # Add the new team
                teams[team_name] = [str(player) for player in players]

                if 'start_post' in round_metadata:
                    # If the round is already open, summarize the changes and ask for confirmation
                    confirm_msg = await ctx.send(
                        f"{round_metadata['round_name']} is already open, so existing solution info may leak between"
                        + " the following players/teams:"
                        + "".join(f"\n - {participants[tag]['name'] if 'name' in participants[tag] else tag}"
                                  + f" ({f'team `{old_team}`' if old_team is not None else 'solo'})"
                                  for tag, old_team in old_teams.items())
                        + f"\nAre you sure you wish to move these players to team `{team_name}`?"
                        + " React with ✅ within 30 seconds to proceed, ❌ to cancel changes to this round.")
                    if not await self.wait_for_confirmation(ctx, confirm_msg):
                        skipped = True
                        continue

                    self.remove_submissions_by(round_dir, puzzle_name, submit_names_to_remove)

                # Write the teams change
                with open(round_dir / 'teams.json', 'w', encoding='utf-8') as f:
                    json.dump(teams, f, ensure_ascii=False, indent=4)

                updated_rounds.append(round_metadata['round_name'])

        if updated_rounds:
            await ctx.send(f"Created team `{team_name}` in {', '.join(updated_rounds)}")
        elif not skipped:
            await ctx.send("No rounds start in the future; specify a starting round to edit already-open ones.")

    @commands.command(name='tournament-team-remove', aliases=['ttr', 'tournament-remove-team', 'trt'])
    @is_host
    async def tournament_remove_team(self, ctx, team_name, from_round=None, only=None):
        """Dissolve a tournament team from the given round onwards.

        team_name: The name of the team to remove.
        from_round: The name of a puzzle/round. The selected team will be removed from
                    all rounds with start dates on or after that round's start date.
                    If not provided, defaults to all rounds starting from the current
                    datetime (i.e. not including any already-open rounds).
        only: If provided, only remove from from_round and not all subsequent rounds.
              It doesn't matter what string you pass here, e.g. 'only'.
        E.g. !tournament-remove-team "A and B" "Round 3" only
        """
        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            if from_round is None:
                from_date = datetime.now(timezone.utc).isoformat()
            else:
                from_puzzle_name = self.get_puzzle_name(tournament_metadata, from_round, is_host=True, missing_ok=False)
                from_date = tournament_metadata['rounds'][from_puzzle_name]['start']

            # Update each relevant round
            updated_rounds = []
            skipped = False
            for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                # Ignore all rounds except specified round if 'only' appended
                if only and puzzle_name != from_puzzle_name:
                    continue

                # Ignore prior rounds and closed rounds
                if round_metadata['start'] < from_date or 'end_post' in round_metadata:
                    continue

                round_dir = tournament_dir / round_metadata['dir']

                with open(round_dir / 'teams.json', encoding='utf-8') as f:
                    teams = json.load(f)

                # Skip this round if the team is not present in it (but continue to check other rounds)
                if team_name not in teams:
                    await ctx.send(f"No team `{team_name}` in {round_metadata['round_name']}")
                    continue

                del teams[team_name]

                if 'start_post' in round_metadata:
                    # If the round is already open, ask for confirmation before removing the team
                    confirm_msg = await ctx.send(
                        f"{round_metadata['round_name']} is already open, are you sure you wish to remove team"
                        + f" `{team_name}` from it?"
                        + " React with ✅ within 30 seconds to proceed, ❌ to cancel changes to this round.")
                    if not await self.wait_for_confirmation(ctx, confirm_msg):
                        skipped = True
                        continue

                    self.remove_submissions_by(round_dir, puzzle_name, {team_name})

                # Write the teams change
                with open(round_dir / 'teams.json', 'w', encoding='utf-8') as f:
                    json.dump(teams, f, ensure_ascii=False, indent=4)

                updated_rounds.append(round_metadata['round_name'])

        if updated_rounds:
            await ctx.send(f"Removed team `{team_name}` from {', '.join(updated_rounds)}")
        elif not skipped:
            await ctx.send("No rounds start in the future; specify a starting round to edit already-open ones.")
