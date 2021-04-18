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
    async def tournament_teams(self, ctx, *, round_or_puzzle_name=None):
        """List all teams in the given round/puzzle. If not specified,
        list all teams that are being persisted to future rounds.

        Note that this does not actually ping the mentioned users.

        round_or_puzzle_name: (Case-insensitive) Return teams in the matching round/puzzle.
                              A string like r10 will also match "Round 10" as a shortcut.
                              If omitted, list all teams that are being persisted to future rounds.
        """
        is_host = is_tournament_host(ctx)
        tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=is_host)

        with open(tournament_dir / 'participants.json', encoding='utf-8') as f:
            participants = json.load(f)

        if not round_or_puzzle_name:
            team_file = tournament_dir / 'teams.json'
            label = 'Ongoing'
        else:
            puzzle_name = self.get_puzzle_name(tournament_metadata, round_or_puzzle_name,
                                               is_host=is_host, missing_ok=False)
            round_metadata = tournament_metadata['rounds'][puzzle_name]
            team_file = tournament_dir / round_metadata['dir'] / 'teams.json'
            label = round_metadata['round_name']

        with open(team_file, encoding='utf-8') as f:
            teams = json.load(f)

        await ctx.send(
            f"{label} teams:\n"
            + '\n'.join(f"  `{team_name}`: "
                        + ', '.join(f"<@{participants[tag]['id']}> ("
                                    + (f"`{participants[tag]['name']}`" if 'name' in participants[tag] else tag)
                                    + ")"
                                    for tag in tags)
                        for team_name, tags in teams.items()),
            allowed_mentions=discord.AllowedMentions(users=False))

    @commands.command(name='tournament-team-add', aliases=['tta', 'tournament-add-team', 'tat',
                                                           'tournament-team-create', 'tournament-create-team'])
    @is_host
    async def tournament_create_team(self, ctx, team_name, from_round, *players: discord.User):
        """Create a tournament team from the given discord users.

        If the team name already exists, members will be added or removed to match the given new list (and the team's
        submissions will not be removed).

        team_name: The name of the team.
        from_round: The name of an *open* puzzle/round, or an empty string (""). The
                    selected players will be put in a team for all rounds starting
                    from the given round's start date, or to all future rounds if an
                    empty string is given.
                    Future rounds can't be passed, to avoid arbitrary time ranges on
                    teams for newly-added puzzles.
        players: The discord users to include in the given team. If they were already
                 in a team for a currently-open round, confirmation will be asked.
        """
        assert len(players) > 1, "Team must have at least 2 players!"

        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            if from_round:
                from_puzzle = self.get_puzzle_name(tournament_metadata, from_round, is_host=True, missing_ok=False)

                # In order to avoid having to store complex datetime-dependent conditions for teams that get applied
                # to newly-added puzzles, we only allow teams to change from an open puzzle or the current datetime.
                # In other words, only from dates in the past, so that we have only one non-conflicting set of teams
                # to store for new puzzles.
                if 'start_post' not in tournament_metadata['rounds'][from_puzzle]:
                    raise Exception('Cannot add team from arbitrary future round;'
                                    'use "" for round_name to modify all future rounds.')

                from_date = tournament_metadata['rounds'][from_puzzle]['start']
            else:
                from_date = datetime.now(timezone.utc).isoformat()

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

                # Add the new team and re-sort by team name, case-insensitively
                teams[team_name] = [str(player) for player in players]
                teams = {k: teams[k] for k in sorted(teams, key=lambda s: s.lower())}

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
                        continue

                    self.remove_submissions_by(round_dir, puzzle_name, submit_names_to_remove)

                # Write the teams change
                with open(round_dir / 'teams.json', 'w', encoding='utf-8') as f:
                    json.dump(teams, f, ensure_ascii=False, indent=4)

                updated_rounds.append(round_metadata['round_name'])

            # Update the 'current' teams (the teams that get applied to newly-added puzzles)
            with open(tournament_dir / 'teams.json', encoding='utf-8') as f:
                teams = json.load(f)

            teams[team_name] = [str(player) for player in players]

            # Remove these players from any other teams they're in
            for tag in teams[team_name]:
                for other_team_name, other_team_tags in teams.items():
                    if other_team_name != team_name and tag in other_team_tags:
                        other_team_tags.remove(tag)

                        # If this reduces the team size to 1, remove it entirely
                        if len(other_team_tags) <= 1:
                            del teams[other_team_name]  # Should be safe since we're breaking out of the loop now anyway

                        break

            with open(tournament_dir / 'teams.json', 'w', encoding='utf-8') as f:
                json.dump(teams, f, ensure_ascii=False, indent=4)

        if updated_rounds:
            await ctx.send(f"Set team `{team_name}` in {', '.join(f'`{r}`' for r in updated_rounds)} and future rounds.")
        else:
            await ctx.send(f"Set team `{team_name}` for future rounds.")

    @commands.command(name='tournament-team-delete', aliases=['ttd', 'tournament-delete-team', 'tdt'])
    @is_host
    async def tournament_delete_team(self, ctx, team_name, from_round=None, only=None):
        """Dissolve a tournament team from the given round onwards.

        team_name: The name of the team to remove.
        from_round: The name of a puzzle/round. The selected team will be removed
                    from all rounds with start dates on or after that round's start date.
                    If not provided, defaults to all rounds starting from the current
                    datetime (i.e. not including any already-open rounds).
                    If the 'only' arg is not provided, a non-open round may not be chosen
                    (this avoids complexities with arbitrary time ranges for future teams).
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

                # In order to avoid having to store complex datetime-dependent conditions for teams that get applied
                # to newly-added puzzles, we only allow teams to change from an open puzzle or the current datetime.
                # In other words, only to dates in the past, so that we have only one non-conflicting set of teams
                # to store for new puzzles.
                if 'start_post' not in tournament_metadata['rounds'][from_puzzle_name] and not only:
                    raise Exception("Cannot specify a future round unless `only` argument is added.")

                from_date = tournament_metadata['rounds'][from_puzzle_name]['start']

            # Update each relevant round
            updated_rounds = []
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
                        continue

                    self.remove_submissions_by(round_dir, puzzle_name, {team_name})

                # Write the teams change
                with open(round_dir / 'teams.json', 'w', encoding='utf-8') as f:
                    json.dump(teams, f, ensure_ascii=False, indent=4)

                updated_rounds.append(round_metadata['round_name'])

            # Update the 'current' teams (the teams that get applied to newly-added puzzles)
            if not only:
                with open(tournament_dir / 'teams.json', encoding='utf-8') as f:
                    teams = json.load(f)

                if team_name in teams:
                    del teams[team_name]
                    with open(tournament_dir / 'teams.json', 'w', encoding='utf-8') as f:
                        json.dump(teams, f, ensure_ascii=False, indent=4)
                elif not updated_rounds:
                    raise Exception(f"No team named `{team_name}` found in specified or future rounds.")

        if updated_rounds:
            await ctx.send(f"Removed team `{team_name}` from {', '.join(f'`{r}`' for r in updated_rounds)} and future rounds.")
        else:
            await ctx.send(f"Removed team `{team_name}` from future rounds.")

    @commands.command(name='tournament-team-rename', aliases=['ttr', 'tournament-rename-team', 'trt'])
    @is_host
    async def rename_team(self, ctx, team_name, new_team_name):
        """Rename a team.

        All players in the team (for any unclosed round) will be DM'd to inform them of the change.

        team_name: The name of the team.
        new_team_name: The new name of the team.
        """
        async with self.tournament_metadata_write_lock:
            tournament_dir, tournament_metadata = self.get_active_tournament_dir_and_metadata(is_host=True)

            with open(tournament_dir / 'participants.json', 'r', encoding='utf-8') as f:
                participants = json.load(f)

            # First make sure the new team name doesn't conflict with any player nicknames
            if any(team_name == player_info['name'] for player_info in participants.values()
                   if 'name' in player_info):
                raise ValueError(f"`{team_name}` is already in use as a player's nickname.")

            # Also make sure the new team name doesn't conflict with any team names from unclosed rounds
            teams_dicts = {}
            for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                # Ignore closed rounds
                if 'end_post' not in round_metadata:
                    round_dir = tournament_dir / round_metadata['dir']
                    with open(round_dir / 'teams.json', encoding='utf-8') as f:
                        teams_dicts[puzzle_name] = json.load(f)

                    if new_team_name in teams_dicts[puzzle_name]:
                        raise Exception(f"Team named `{new_team_name}` already exists in {round_metadata['round_name']}")

            # Also check the ongoing teams
            with open(tournament_dir / 'teams.json', encoding='utf-8') as f:
                teams_dicts[None] = json.load(f)

            if new_team_name in teams_dicts[None]:
                raise Exception(f"Team named `{new_team_name}` already exists in ongoing teams.")

            # Once we've confirmed there are no name conflicts, update the team name in all open and future rounds
            discord_tags_to_dm = set()
            for puzzle_name, round_metadata in tournament_metadata['rounds'].items():
                # Ignore closed rounds
                if 'end_post' in round_metadata:
                    continue

                round_dir = tournament_dir / round_metadata['dir']
                teams = teams_dicts[puzzle_name]

                if team_name not in teams:
                    continue

                # Update submissions and history
                if 'start_post' in round_metadata:
                    self.rename_submissions_by(round_dir, puzzle_name, team_name, new_team_name)
                    self.rename_author_in_history(round_dir, puzzle_name, team_name, new_team_name)

                # Update the teams json and re-sort by team name, case-insensitively
                teams[new_team_name] = teams[team_name]
                del teams[team_name]
                teams = {k: teams[k] for k in sorted(teams, key=lambda s: s.lower())}

                for tag in teams[new_team_name]:
                    discord_tags_to_dm.add(tag)

                with open(round_dir / 'teams.json', 'w', encoding='utf-8') as f:
                    json.dump(teams, f, ensure_ascii=False, indent=4)

            # Update the ongoing teams dict (the teams that get applied to newly-added puzzles)
            teams = teams_dicts[None]

            if team_name in teams:
                teams[new_team_name] = teams[team_name]
                del teams[team_name]
                teams = {k: teams[k] for k in sorted(teams, key=lambda s: s.lower())}

                for tag in teams[new_team_name]:
                    discord_tags_to_dm.add(tag)

                with open(tournament_dir / 'teams.json', 'w', encoding='utf-8') as f:
                    json.dump(teams, f, ensure_ascii=False, indent=4)

        if not discord_tags_to_dm:
            raise Exception(f"No team `{team_name}` in current or future rounds.")

        # DM the team members to let them know of the new name
        with open(tournament_dir / 'participants.json', encoding='utf-8') as f:
            participants = json.load(f)

        for tag in discord_tags_to_dm:
            player = await self.bot.fetch_user(participants[tag]['id'])
            await player.send(f"The tournament host has renamed your team `{team_name}` to `{new_team_name}`.")

        await ctx.send(f"Renamed team `{team_name}` to `{new_team_name}` in all open and future rounds,"
                       " and DM'd all team members to inform them.")
