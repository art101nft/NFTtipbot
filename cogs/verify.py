import asyncio
import sys
import traceback

import discord
from discord import app_commands
from discord.ext import commands
from cogs.utils import Utils


class Verification(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.utils = Utils(self.bot)

    @app_commands.command(
        name="nftverify",
        description="Verify your 0x address with NFT Bot and Discord."
    )
    async def command_browse(
        self,
        interaction: discord.Interaction,
        secret: str=None
    ) -> None:
        """ /nftverify [secret]"""
        await interaction.response.send_message(f"{interaction.user.mention} checking verification...", ephemeral=True)

        get_user_info = await self.utils.get_user_info(str(interaction.user.id), self.bot.server_bot)
        if get_user_info is None:
            await interaction.edit_original_response(content=self.bot.first_message)
            await self.utils.insert_user_info(str(interaction.user.id), self.bot.server_bot, 1)
            return
        else:
            if get_user_info['is_frozen'] == 1:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, please contact Bot's dev. Your account is currently locked."
                )
                return
        await self.utils.insert_user_info(str(interaction.user.id), self.bot.server_bot, 1)

        try:
            verify = await self.utils.verification_check(str(interaction.user.id), self.bot.server_bot)
            if secret and len(verify) < self.bot.config['wallet']['max_verified_per_user']:
                # if he gave secret..
                get_from_sk = await self.utils.verification_find_sk(secret)
                if get_from_sk is None:
                    await interaction.edit_original_response(
                        content=f"{interaction.user.mention}, could not find any pending verification with `{secret}`."
                    )
                else:
                    # verify him
                    verified = await self.utils.verification_update(
                        str(interaction.user.id),
                        "{interaction.user.name}#{interaction.discriminator}",
                        self.bot.server_bot,
                        get_from_sk['secret_key']
                    )
                    if verified is True:
                        await interaction.edit_original_response(
                            content=f"{interaction.user.mention}, you verified with an address "
                                    f"`{get_from_sk['address']}` by key `{secret}`."
                        )
                return
            if len(verify) > 0:
                address = []
                for each in verify:
                    address.append(each['address'])
                address_list = address[0]
                if len(address) > 0:
                    address_list = ", ".join(address)
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, you already verified with address `{address_list}`."
                )
            else:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, please sign in with Discord and MetaMask here "
                            f"<{self.bot.config['discord']['verify_link']}>. "
                            f"(There is no input for seed or key or password at all)."
                )
            return
        except Exception:
            traceback.print_exc(file=sys.stdout)

    async def cog_load(self) -> None:
        pass

    async def cog_unload(self) -> None:
        asyncio.ensure_future(self.site.stop())


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Verification(bot))

