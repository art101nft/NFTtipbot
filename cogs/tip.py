import sys
import traceback
from typing import List
import random
from datetime import datetime

import discord
from discord import app_commands
from discord.ext import commands
from cogs.utils import Utils, truncate


class Tip(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.utils = Utils(self.bot)

    @app_commands.guild_only()
    @app_commands.command(
        name="gastip",
        description="Tip your gas to other Discord user."
    )
    async def command_gas_tip(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        amount: str,
        gas_ticker: str
    ) -> None:
        """ /gastip <member> <amount> <ETH or MATIC> """
        await interaction.response.send_message(f"{interaction.user.mention} loading gas tip...")

        if self.bot.config['wallet']['enable_gas_tip'] != 1 or self.bot.confog['wallet']['use_community_gas'] == 1:
            await interaction.edit_original_response(
                content=f"{interaction.user.mention}, gas tip is currently disable or community gas is enable."
            )
            return
        amount = amount.replace(",", "")
        original_amount = amount
        truncate_info = ""
        try:
            amount = float(amount)
            original_amount = amount
            amount = truncate(amount, 4) # limit 4 decimal
            if float(original_amount) > float(amount):
                truncate_info = " Amount is truncated."
        except ValueError:
            return

        if amount <= 0:
            await interaction.edit_original_response(
                content=f"{interaction.user.mention},{truncate_info} amount can't be zero or negative."
            )
            return

        gas_ticker = gas_ticker.lower()
        if gas_ticker not in ["eth", "matic"]:
            await interaction.edit_original_response(
                content=f"{interaction.user.mention}, Only ETH and MATIC."
            )
            return

        max_gas = self.bot.config['max_gas_tip'][gas_ticker]
        if amount > max_gas:
            await interaction.edit_original_response(
                content=f"{interaction.user.mention}, amount can't be bigger than `{str(max_gas)} {gas_ticker}`."
            )
            return

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

        if member.id == interaction.user.id:
            await interaction.edit_original_response(
                content=f"{interaction.user.mention}, no.... You can tip me if you want."
            )
            return
        # if amount is bigger than his
        key_ticker = gas_ticker + "_gas"
        user_balance = get_user_info[key_ticker]
        if user_balance < amount:
            await interaction.edit_original_response(
                content=f"{interaction.user.mention}, no.... you don't have sufficient amount to do so."
            )
            return
        else:
            # check if receiver has record
            get_receiver_info = await self.utils.get_user_info(str(member.id), self.bot.server_bot)
            if get_receiver_info is None:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, User {member.mention} is not in our database yet."
                )
                return
            else:
                if get_receiver_info['is_frozen'] == 1:
                    await interaction.edit_original_response(
                        content=f"{interaction.user.mention}, User {member.mention} is currently locked."
                    )
                    return
                else:
                    # move tip
                    move_gas = await self.utils.move_gas(
                        str(interaction.user.id), self.bot.server_bot, str(member.id), self.bot.server_bot,
                        amount, gas_ticker
                    )
                    if move_gas is True:
                        await interaction.edit_original_response(
                            content=f"{interaction.user.mention}, successfully transferred gas {str(amount)} {gas_ticker} to {member.mention}.{truncate_info}"
                        )
                    else:
                        await interaction.edit_original_response(
                            content=f"{interaction.user.mention}, internal error."
                        )

    @app_commands.guild_only()
    @app_commands.command(
        name="nftip",
        description="Tip your fund NFT to other Discord user."
    )
    async def command_tip(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        item_id: str,
        collection: str=None
    ) -> None:
        """ /nftip <member> <item id> <collection> """
        await interaction.response.send_message(f"{interaction.user.mention} loading tip...")

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

        if member.id == interaction.user.id:
            await interaction.edit_original_response(
                content=f"{interaction.user.mention}, no.... You can tip me if you want."
            )
            return

        try:
            amount = 1 # can change later if tip more than 1
            # check if he's still own it when tipping
            list_user_assets = await self.utils.get_list_user_assets(str(interaction.user.id), self.bot.server_bot)
            if len(list_user_assets) == 0:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, you don't own any NFT."
                )
            else:
                list_item_ids = [int(each['nft_id']) for each in list_user_assets]
                if int(item_id) not in list_item_ids:
                    await interaction.edit_original_response(
                        content=f"{interaction.user.mention}, that NFT doesn't belong to you."
                    )
                else:
                    # get item detail
                    contract_id = None
                    for each in list_user_assets:
                        if each['nft_id'] == int(item_id):
                            contract_id = each['contract_id']
                            contract = each['contract']
                            collection_name = each['collection_name']
                            item_name = each['name']
                            network = each['network']
                            token_id_hex = each['token_id_hex']
                            break
                    # if collection is given but item id is not in those collections, reject
                    if collection:
                        get_collection = await self.utils.get_contract_by_id(int(collection))
                        if get_collection and get_collection['contract_id'] != contract_id:
                            given_collection_name = get_collection['collection_name']
                            await interaction.edit_original_response(
                                content=f"{interaction.user.mention}, incorrect given collection "
                                        f"`{given_collection_name}` but item is in `{collection_name}`. OR do not input it."
                            )
                            return
                    token_id_int = int(token_id_hex, 16) if int(token_id_hex, 16) < 10**20 else None
                    mv_tip = await self.utils.move_nft(
                        network, contract, str(interaction.user.id), self.bot.server_bot, 
                        str(member.id), self.bot.server_bot, token_id_int, token_id_hex,
                        contract_id, amount
                    )
                    if mv_tip is True:
                        await interaction.edit_original_response(
                            content=f"{interaction.user.mention}, you tipped {item_name} / {collection_name} "
                                    f"to {member.mention}."
                        )
                    else:
                        await interaction.edit_original_response(
                            content=f"{interaction.user.mention}, internal error."
                        )
        except Exception:
            traceback.print_exc(file=sys.stdout)

    @command_tip.autocomplete('collection')
    async def collection_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str
    ) -> List[app_commands.Choice[str]]:
        # Do stuff with the "current" parameter, e.g. querying it search results...
        list_user_assets = await self.utils.get_list_user_assets(str(interaction.user.id), self.bot.server_bot)
        collectin_list = []
        collectin_list_ids = []
        if len(list_user_assets) > 0:
            for each in list_user_assets:
                if str(each['contract_id']) not in collectin_list_ids:
                    collectin_list.append(each['collection_name'])
                    collectin_list_ids.append(str(each['contract_id']))
            return [
                app_commands.Choice(name=item, value=collectin_list_ids[collectin_list.index(item)])
                for item in collectin_list if current.lower() in item.lower()
            ]
        else:
            collectin_list = ["N/A"]
            return [
                app_commands.Choice(name=item, value=item)
                for item in collectin_list if current.lower() in item.lower()
            ]

    @command_tip.autocomplete('item_id')
    async def item_id_callback(
        self,
        interaction: discord.Interaction,
        current: str
    ) -> List[app_commands.Choice[str]]:
        # Do stuff with the "current" parameter, e.g. querying it search results...
        list_user_assets = await self.utils.get_list_user_assets_search(
            str(interaction.user.id), self.bot.server_bot, current, 10
        )
        item_list = []
        asset_ids = []
        uniq_set = []
        if len(list_user_assets) > 0:
            for each in list_user_assets:
                if each['contract'].lower() + each['token_id_hex'] + each['network'] not in uniq_set:
                    item_list.append(each['name'])
                    asset_ids.append(str(each['nft_id']))
        else:
            item_list = ["N/A"]
        return [
            app_commands.Choice(name=item, value=asset_ids[item_list.index(item)])
            for item in item_list if current.lower() in item.lower()
        ]

    @app_commands.guild_only()
    @app_commands.command(
        name="pickuser",
        description="Pick a user from who re-act on a message."
    )
    async def command_pickuser(
        self,
        interaction: discord.Interaction,
        msg_id: str,
        emoji_str: str,
        channel: discord.TextChannel=None
    ) -> None:
        """ /pickuser <message ID> <emoji> """
        await interaction.response.send_message(f"{interaction.user.mention} finding message...")    
        try:
            msg_id = int(msg_id)
            try:
                if channel is not None:
                    _msg: discord.Message = await channel.fetch_message(msg_id)
                else:
                    _msg: discord.Message = await interaction.channel.fetch_message(msg_id)
            except discord.errors.NotFound:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, message not found or wrong channel."
                )
                return

            if _msg is None:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, I can't find that message `{str(msg_id)}`. Try again later or double check the ID."
                )
            else:
                # check if that's his message
                if _msg.author != interaction.user:
                    await interaction.edit_original_response(
                        content=f"{interaction.user.mention}, you can only do with your message. Try again!"
                    )
                    return
                else:
                    my_emoji = discord.PartialEmoji.from_str(emoji_str)
                    if my_emoji is None:
                        await interaction.edit_original_response(
                            content=f"{interaction.user.mention}, I can't get emoji from string `{emoji_str}`!"
                        )
                        return

                    attend_list = []
                    if len(_msg.reactions) > 0:
                        for each_r in _msg.reactions:
                            print("{} vs {}".format(each_r, emoji_str))
                            if str(each_r) == str(emoji_str):
                                attend_list = [user async for user in each_r.users() if not user.bot and user != interaction.user]
                            else:
                                continue
                        if len(attend_list) > 1:
                            random.seed(datetime.now())
                            picked_u = random.choice(attend_list)
                            await interaction.edit_original_response(
                                content=f"{interaction.user.mention}, there are total {str(len(attend_list))} users. A random selected {picked_u.mention} by {interaction.user.mention}."
                            )
                        else:
                            await interaction.edit_original_response(
                                content=f"{interaction.user.mention}, need more user to re-act on that mesage with {emoji_str}."
                            )
                    else:
                        await interaction.edit_original_response(
                            content=f"{interaction.user.mention}, I can't find anyone re-act with {emoji_str}."
                        )
        except ValueError:
            await interaction.edit_original_response(
                content=f"{interaction.user.mention}, invalid message ID."
            )
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    async def cog_load(self) -> None:
        pass

    async def cog_unload(self) -> None:
        pass


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Tip(bot))
