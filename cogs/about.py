import sys
import traceback
from datetime import datetime

import discord
from discord import app_commands
from discord.ext import commands

from cogs.utils import Utils
from cogs.utils import eth_wallet_getbalance


class About(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.utils = Utils(self.bot)

    async def async_about(
        self, interaction
    ):
        msg = await interaction.send(f"{interaction.author.mention} loading about...")
        try:
            await msg.edit(content=None, embed=await self.about_embed(interaction.author.name))
        except Exception:
            traceback.print_exc(file=sys.stdout)

    async def async_slash_about(
        self, interaction
    ):
        await interaction.response.send_message(f"{interaction.user.mention} loading about...")
        try:
            await interaction.edit_original_response(content=None, embed=await self.about_embed(interaction.user.name))
        except Exception:
            traceback.print_exc(file=sys.stdout)

    async def about_embed(self, requested_by):
        description = ""
        try:
            guilds = '{:,.0f}'.format(len(self.bot.guilds))
            total_members = '{:,.0f}'.format(sum(1 for m in self.bot.get_all_members()))
            total_unique = '{:,.0f}'.format(len(self.bot.users))
            total_bots = '{:,.0f}'.format(sum(1 for m in self.bot.get_all_members() if m.bot is True))
            total_online = '{:,.0f}'.format(sum(1 for m in self.bot.get_all_members() if
                                                m.status != discord.Status.offline))

            description = "Total guild(s): `{}` Total member(s): `{}`\n" \
                "Unique: `{}` Bots: `{}`\n" \
                "Online: `{}`\n\n".format(guilds, total_members, total_unique, total_bots, total_online)
        except Exception:
            traceback.print_exc(file=sys.stdout)
        botdetails = discord.Embed(title='About Me', description=description, timestamp=datetime.now())
        try:
            eth_gas = await eth_wallet_getbalance(self.bot.config['endpoint']['ethereum'], self.bot.config['wallet']['eth_address'], None, True)
            matic_gas = await eth_wallet_getbalance(self.bot.config['endpoint']['polygon'], self.bot.config['wallet']['eth_address'], None, True) 

            if eth_gas > 0 or matic_gas > 0:
                botdetails.add_field(name="Bot's Gas",
                                     value="`{:,.5f}` ETH / `{:,.5f}` MATIC".format(
                                         eth_gas/10** self.bot.config['gas_decimal']['ethereum'],
                                         matic_gas/10**self.bot.config['gas_decimal']['polygon']
                                     ),
                                     inline=False)
        except Exception:
            traceback.print_exc(file=sys.stdout)
        botdetails.add_field(name="Github", value=self.bot.config['other']['github_link'])
        botdetails.set_footer(text=f'Made in Python | requested by {requested_by}',
                              icon_url='http://findicons.com/files/icons/2804/plex/512/python.png')
        botdetails.set_author(name=self.bot.user.name, icon_url=self.bot.user.display_avatar)
        return botdetails

    @commands.command(
        name='about',
        description="Show about and info."
    )
    async def command_about(
        self, ctx: commands.Context
    ) -> None:
        """ /about """
        await self.async_about(ctx)

    @app_commands.command(
        name='about',
        description="Show about and info."
    )
    async def slash_about(
        self, interaction: discord.Interaction
    ) -> None:
        """ /about """
        await self.async_slash_about(interaction)

    @app_commands.command(
        name="nfirst",
        description="Show first start message."
    )
    async def command_first(
        self, interaction: discord.Interaction
    ) -> None:
        """ /nfirst """
        await interaction.response.send_message(
            self.bot.first_message.replace("Hello, it looks like it's your first time interact with our NFT Bot. ", "")
        )

    async def cog_load(self) -> None:
        pass

    async def cog_unload(self) -> None:
        pass


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(About(bot))
