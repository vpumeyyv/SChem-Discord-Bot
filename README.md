# SChem Discord Bot

Discord bot for hosting tournaments and running solutions for SpaceChem (https://www.zachtronics.com/spacechem), via
the SChem package (https://pypi.org/project/schem/).

## Setup
Add:
```dotenv
SCHEM_BOT_DISCORD_TOKEN=<your bot API token>
SCHEM_BOT_ANNOUNCEMENTS_CHANNEL_ID=<public tournament channel ID>
SCHEM_BOT_ADMIN_ID=<user ID of bot admin; can set tournament hosts>
```
to a `.env` file in the same directory as bot.py

See https://discordpy.readthedocs.io/en/latest/discord.html for setting up the bot and getting its API token and 
https://support.discord.com/hc/en-us/articles/206346498-Where-can-I-find-my-User-Server-Message-ID- for getting the channel and user IDs.

## Usage
`python bot.py`

Check the bot is running and see available commands by sending `!help` in either a DM to the bot or the public channel.
Note that DMs to the bot can only be sent if you are in the same server as the bot.

Tournament files will be written to `tournaments/` in the same directory as bot.py

Warning: Do not modify tournament files while the bot is running. It is recommended
to setup and view info on the tournament via the corresponding bot commands.
