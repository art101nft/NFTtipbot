import json
import sys
import time
import traceback
from datetime import datetime
from typing import List

import discord
from discord import app_commands
from discord.ext import commands, tasks

from cogs.utils import Utils
from cogs.utils import check_address, \
    transfer_nft, eth_get_tx_info, truncate


class Deposit(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.utils = Utils(self.bot)

    @app_commands.command(
        name="nftransfer",
        description="Transfer your NFT to external address."
    )
    async def command_transfer(
        self,
        interaction: discord.Interaction,
        address: str,
        nft_id: str
    ) -> None:
        """ /nftransfer <address> <item id>"""
        await interaction.response.send_message(f"{interaction.user.mention} loading withdraw...", ephemeral=True)

        # Check if withdraw is enable
        if self.bot.config['wallet']['enable_withdraw'] != 1:
            await interaction.edit_original_response(
                content=f"{interaction.user.mention}, withdraw is currently disable. Try again later!")
            return
        get_user_info = await self.utils.get_user_info(str(interaction.user.id), self.bot.server_bot)
        if get_user_info is None:
            await interaction.edit_original_response(content=self.bot.first_message)
            await self.utils.insert_user_info(str(interaction.user.id), self.bot.server_bot, 1)
            return
        else:
            if get_user_info['is_frozen'] == 1:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, please contact Bot's dev. Your account is currently locked.")
                return
        await self.utils.insert_user_info(str(interaction.user.id), self.bot.server_bot, 1)

        try:
            # check address
            if not check_address(address):
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, address `{address}` is invalid.")
                return
            # check if he's still own it when withdraw
            list_user_assets = await self.utils.get_list_user_assets(str(interaction.user.id), self.bot.server_bot)
            if len(list_user_assets) == 0:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, you don't own any NFT."
                )
                await self.utils.log_to_channel(
                    self.bot.config['discord']['log_channel'],
                    f"Reject {str(interaction.user.id)} / {interaction.user.name} item id `{str(nft_id)}`."\
                    f" He/she has no NFT!"
                )
            else:
                list_item_ids = [int(each['nft_id']) for each in list_user_assets]
                if int(nft_id) not in list_item_ids:
                    await interaction.edit_original_response(
                        content=f"{interaction.user.mention}, that NFT doesn't belong to you.")
                    await self.utils.log_to_channel(
                        self.bot.config['discord']['log_channel'],
                        f"Reject {str(interaction.user.id)} / {interaction.user.name} item id `{str(nft_id)}`."\
                        f" Not belong to him/her!"
                    )
                else:
                    # get item detail
                    # contract_id = None
                    contract = None
                    token_id = None
                    network = None
                    for each in list_user_assets:
                        if each['nft_id'] == int(nft_id):
                            contract = each['contract']
                            contract_type = each['contract_type']
                            # contract_id = each['contract_id']
                            collection_name = each['collection_name']
                            item_name = each['name']
                            token_id = int(each['token_id_hex'], 16)
                            network = each['network']
                            break
                    if contract is None or token_id is None or network is None:
                        await interaction.edit_original_response(
                            content=f"{interaction.user.mention}, failed to get item's detail.")
                        return
                    # check if there is pending gas when withdraw
                    withdraw_pending = await self.utils.get_pending_withdraw_tx_list_by_id_user(
                        str(interaction.user.id), self.bot.server_bot, network
                    )
                    if len(withdraw_pending) > 0:
                        await interaction.edit_original_response(
                            content=f"{interaction.user.mention}, there is still a pending tx you did earlier. "
                                    f"Please try again later!")
                        await self.utils.log_to_channel(
                            self.bot.config['discord']['log_channel'],
                            f"Reject {str(interaction.user.id)} / {interaction.user.name} withdraw: `{item_name}` "
                            f"/ `{str(token_id)}`. Pending clearing his/her own previous Tx!"
                        )
                        return

                    # check if nonce for withdraw address
                    pending_withdraw_tx_list = await self.utils.get_pending_withdraw_tx_list(network)
                    if len(pending_withdraw_tx_list) > 0:
                        await interaction.edit_original_response(
                            content=f"{interaction.user.mention}, there are some pending tx with fund wallet. "
                                    f"Please try again later!"
                        )
                        await self.utils.log_to_channel(
                            self.bot.config['discord']['log_channel'],
                            f"Reject {str(interaction.user.id)} / {interaction.user.name} withdraw: `{item_name}` "
                            f"/ `{str(token_id)}`. Pending clearing Tx for network `{network}`!"
                        )
                        return
                    # process
                    try:
                        min_gas = 0.005 # Just leave it as default
                        your_gas = 0.0
                        await self.utils.get_bot_setting()
                        url = self.bot.config['endpoint'][network.lower()]
                        if network == "ETHEREUM":
                            your_gas = get_user_info['eth_gas']
                            min_gas = json.loads(self.bot.setting['min_gas_move_nft'])['ETH']
                            chain_id = 1
                        elif network == "POLYGON":
                            your_gas = get_user_info['matic_gas']
                            min_gas = json.loads(self.bot.setting['min_gas_move_nft'])['MATIC']
                            chain_id = 137
                        else:
                            await interaction.edit_original_response(
                                content=f"{interaction.user.mention}, can't get network endpoint.")
                            return

                        if your_gas < min_gas:
                            min_gas_str = "{:,.4f}".format(min_gas)
                            having_gas_str = "{:,.4f}".format(your_gas)
                            await interaction.edit_original_response(
                                content=f"{interaction.user.mention}, you don't have enough reserved gas. "
                                        f"Having {having_gas_str}, and required {min_gas_str}.")
                            await self.utils.log_to_channel(
                                self.bot.config['discord']['log_channel'],
                                f"Reject {str(interaction.user.id)} / {interaction.user.name} withdraw: "
                                f"`{item_name}` / `{str(token_id)}`. Shortage of user's gas!"
                            )
                            return
                    except Exception:
                        traceback.print_exc(file=sys.stdout)
                        await interaction.edit_original_response(
                            content=f"{interaction.user.mention}, internal error with network.")
                        return
                    # withdraw
                    withdraw_tx = None
                    if contract_type == "ERC721":
                        withdraw_tx = await transfer_nft(
                            url, "ERC721", contract, self.bot.ERC721_ABI, self.bot.config['wallet']['eth_address'],
                            address, self.bot.config['wallet']['eth_key'], token_id, chain_id, 1
                        ) # amount 1
                    else:
                        withdraw_tx = await transfer_nft(
                            url, "ERC1155", contract, self.bot.ERC1155_ABI, self.bot.config['wallet']['eth_address'],
                            address, self.bot.config['wallet']['eth_key'], token_id, chain_id, 1
                        ) # amount 1
                    if withdraw_tx is None:
                        await interaction.edit_original_response(
                            content=f"{interaction.user.mention}, internal error during withdraw."
                        )
                        await self.utils.log_to_channel(
                            self.bot.config['discord']['log_channel'],
                            f"[DISCORD] {str(interaction.user.id)} / {interaction.user.name} failed to withdraw: `{item_name}` / `{str(token_id)}`."
                        )
                    else:
                        try:
                            await self.utils.transferred_nft(
                                network, contract, hex(token_id), str(interaction.user.id), withdraw_tx,
                                self.bot.server_bot, address, 1
                            ) # amount is 1
                            await interaction.edit_original_response(
                                content=f"{interaction.user.mention}, you withdrew {item_name} / {collection_name} "
                                        f"to `{address}` with tx hash `{withdraw_tx}`. Wait for confirmation!")
                        except Exception:
                            traceback.print_exc(file=sys.stdout)
                        await self.utils.log_to_channel(
                            self.bot.config['discord']['log_channel'],
                            f"[DISCORD] {str(interaction.user.id)} / {interaction.user.name} withdrew `{item_name}`. Tx: `{withdraw_tx}`"
                        )
        except Exception:
            traceback.print_exc(file=sys.stdout)

    @command_transfer.autocomplete('nft_id')
    async def nft_id_callback(
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
                app_commands.Choice(name=item, value=item)
                for item in item_list if current.lower() in item.lower()
            ]
        return [
            app_commands.Choice(name=item, value=asset_ids[item_list.index(item)])
            for item in item_list if current.lower() in item.lower()
        ]

    @app_commands.command(
        name="nftbalance",
        description="Show your NFT in fund wallet."
    )
    async def command_balance(
        self, interaction: discord.Interaction
    ) -> None:
        """ /nftbalance """
        await interaction.response.send_message(f"{interaction.user.mention} loading balance...", ephemeral=True)

        get_user_info = await self.utils.get_user_info(str(interaction.user.id), self.bot.server_bot)
        if get_user_info is None:
            await interaction.edit_original_response(content=self.bot.first_message)
            await self.utils.insert_user_info(str(interaction.user.id), self.bot.server_bot, 1)
            return
        else:
            if get_user_info['is_frozen'] == 1:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, please contact Bot's dev. "
                            f"Your account is currently locked.")
                return
        await self.utils.insert_user_info(str(interaction.user.id), self.bot.server_bot, 1)

        try:
            list_user_assets = await self.utils.get_list_user_assets(str(interaction.user.id), self.bot.server_bot)
            if len(list_user_assets) == 0:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, you don't have any NFT in your fund wallet."
                )
            else:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, you have {str(len(list_user_assets))} NFTs "
                            f"in your fund wallet."
                )
                try:
                    embed = discord.Embed(
                        title='Your NFT Balance',
                        description=f"{interaction.user.mention}, you have {str(len(list_user_assets))} "
                                    f"NFTs in your fund wallet. Use `/nftbrowse` to see your NFTs.",
                        timestamp=datetime.now())
                    embed.set_author(name=interaction.user.name, icon_url=interaction.user.display_avatar)
                    embed.set_footer(text=f'Requested by {interaction.user.name}#{interaction.user.discriminator}',
                                     icon_url=interaction.user.display_avatar)
                    list_nfts = {}
                    for each in list_user_assets:
                        if each['collection_name'] not in list_nfts:
                            list_nfts[each['collection_name']] = []
                        if each['contract_type'] == "ERC721":
                            list_nfts[each['collection_name']].append(str(int(each['asset_id'])))
                        elif each['contract_type'] == "ERC1155":
                            list_nfts[each['collection_name']].append(str(int(each['token_id_hex'], 16)))
                    for k, v in list_nfts.items():
                        embed.add_field(name=k,
                                        value=", ".join(v),
                                        inline=False)
                    await interaction.edit_original_response(content=None, embed=embed) # interaction.followup.send
                except Exception:
                    traceback.print_exc(file=sys.stdout)
        except Exception:
            traceback.print_exc(file=sys.stdout)

    @app_commands.command(
        name="nftdeposit",
        description="Show your deposited NFT which not yet transfer to fund wallet."
    )
    async def command_deposit(
        self, interaction: discord.Interaction
    ) -> None:
        """ /nftdeposit """
        await interaction.response.send_message(f"{interaction.user.mention} loading deposit...", ephemeral=True)

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
            embed = discord.Embed(
                title='Deposit',
                description="Please always send from your verified address. "\
                            "Please check with `/nftcollection` to see the supported list of NFTs.",
                timestamp=datetime.now())
            verify = await self.utils.verification_check(str(interaction.user.id), self.bot.server_bot)
            if len(verify) > 0:
                address = []
                for each in verify:
                    address.append(each['address'])
                address_list = address[0]
                address_list = ", ".join(address)
                embed.add_field(name="Only send from verified Address",
                                value=f"```diff\n+ {address_list}```",
                                inline=False)
                embed.add_field(name="Only deposited to",
                                value=f"```diff\n- {self.bot.config['wallet']['eth_address']}```",
                                inline=False)
            else:
                embed.description = "Please use `/nftverify` to to verify your Discord and MetaMask."
                embed.add_field(name="Verified Address",
                                value="N/A (Please verify before deposit or you'll not credited!)",
                                inline=False)
            embed.add_field(name="Your Gas",
                            value="`{:,.5f}` ETH / `{:,.5f}` MATIC".format(get_user_info['eth_gas'],
                                                                           get_user_info['matic_gas']),
                            inline=False)
            try:
                await self.utils.get_bot_setting()
                embed.add_field(name="Supported Networks",
                                value=", ".join(self.bot.config['other']['supported_network']),
                                inline=False)
                try:
                    SLOW = "🐌"
                    NORMAL = "🚴"
                    FAST = "🚀"

                    if "eth_gas_tracker" in self.bot.setting and self.bot.setting['eth_gas_tracker']:
                        gas_data = json.loads(self.bot.setting["eth_gas_tracker"])
                        last_update = discord.utils.format_dt(
                            datetime.fromtimestamp(gas_data['last_update']), style='R'
                        )
                        status_str = "{} {}, {} {}, {} {} {}".format(
                            SLOW, gas_data['SafeGasPrice'], NORMAL, gas_data['ProposeGasPrice'],
                            FAST, gas_data['FastGasPrice'], last_update
                        )
                        embed.add_field(name="ETH Gas Tracker",
                                        value=status_str,
                                        inline=False)
                    if "matic_gas_tracker" in self.bot.setting and self.bot.setting['matic_gas_tracker']:
                        gas_data = json.loads(self.bot.setting["matic_gas_tracker"])
                        last_update = discord.utils.format_dt(
                            datetime.fromtimestamp(gas_data['last_update']), style='R'
                        )
                        status_str = "{} {}, {} {}, {} {} {}".format(
                            SLOW, gas_data['safeLow'], NORMAL, gas_data['standard'],
                            FAST, gas_data['fast'], last_update
                        )
                        embed.add_field(name="MATIC Gas Tracker",
                                        value=status_str,
                                        inline=False)
                except Exception:
                    traceback.print_exc(file=sys.stdout)                
            except Exception:
                traceback.print_exc(file=sys.stdout)
            embed.set_author(name=interaction.user.name, icon_url=interaction.user.display_avatar)
            embed.set_footer(text=f'Requested by {interaction.user.name}#{interaction.user.discriminator}',
                             icon_url=interaction.user.display_avatar)
            await interaction.edit_original_response(content=None, embed=embed)
        except Exception:
            traceback.print_exc(file=sys.stdout)

    @tasks.loop(seconds=20.0)
    async def check_notify_deposit_gas(self):
        await self.bot.wait_until_ready()
        pending_gas = await self.utils.get_confirmed_gas_tx_to_notify()
        if len(pending_gas) > 0:
            for each in pending_gas:
                try:
                    eth_gas = 0.0
                    matic_gas = 0.0
                    url = self.bot.config['endpoint'][each['network'].lower()]
                    amount = each['value']/10**self.bot.config['gas_decimal'][each['network'].lower()]
                    if each['network'] == "ETHEREUM":
                        coin = "ETH"
                        eth_gas = amount
                    elif each['network'] == "POLYGON":
                        coin = "MATIC"
                        matic_gas = amount
                    else:
                        continue
                    check_tx = await eth_get_tx_info(url, each['hash'], 8)
                    status = "CONFIRMED"
                    if check_tx and 'status' in check_tx and int(check_tx['status'], 16) == 0:
                        status = "FAILED" # leave it there
                        continue # skip this tx
                    else:
                        msg = "You have deposited `{:,.6f} {}` with tx `{}` from address: `{}` at height `{}`.".format(
                            amount, coin, each['hash'], each['from_address'], each['block_number']
                        )
                        if each['user_server'] == self.bot.server_bot and each['discord_id'].isdigit():
                            found_user = self.bot.get_user(int(each['discord_id']))
                            if found_user:
                                try:
                                    await found_user.send(msg)
                                except Exception:
                                    traceback.print_exc(file=sys.stdout)
                                await self.utils.update_confirmed_gas_tx_notify(
                                    each['hash'], each['discord_id'], each['user_server'], eth_gas, matic_gas
                                )
                except Exception:
                    traceback.print_exc(file=sys.stdout)

    @tasks.loop(seconds=20.0)
    async def check_notify_deposit_nft(self):
        await self.bot.wait_until_ready()
        # ERC721
        pending_nft = await self.utils.get_confirmed_nft_tx_to_notify()
        if len(pending_nft) > 0:
            for each in pending_nft:
                try:
                    name = each['name']
                    eth_token = 0
                    matic_token = 0
                    amount = each['amount']

                    url = self.bot.config['endpoint'][each['network'].lower()]
                    if each['network'] == "ETHEREUM":
                        eth_token = each['amount']
                    elif each['network'] == "POLYGON":
                        matic_token = each['amount']
                    else:
                        continue

                    if each['amount'] > 1000:
                        print("Skipped tx {} on network: {} for amount token ID {}".format(
                            each['transaction_hash'], each['network'], each['amount'])
                        )
                        continue

                    crediting = (
                        each['network'], each['token_address'],
                        each['token_id_int'], each['token_id_hex'], each['discord_id'], 
                        each['user_server'], amount
                    )

                    check_tx = await eth_get_tx_info(url, each['transaction_hash'], 8)
                    status = "CONFIRMED"
                    if check_tx and 'status' in check_tx and int(check_tx['status'], 16) == 0:
                        status = "FAILED" # leave it there
                        continue # skip failed tx
                    else:
                        msg = "You have deposited `{}` of `{}` with tx `{}` from address: `{}` " \
                              "at height `{}` on network `{}`.".format(each['amount'], name, each['transaction_hash'],
                                                                       each['from_address'], each['block_number'],
                                                                       each['network']
                        )
                        if each['user_server'] == self.bot.server_bot and each['discord_id'].isdigit():
                            found_user = self.bot.get_user(int(each['discord_id']))
                            if found_user:
                                try:
                                    await found_user.send(msg)
                                except Exception:
                                    traceback.print_exc(file=sys.stdout)
                                await self.utils.insert_nft_deposited_credits(
                                    each['discord_id'], each['user_server'], 
                                    eth_token, matic_token, crediting, each['nft_tx_id'])
                except Exception:
                    traceback.print_exc(file=sys.stdout)
        # ERC1155
        pending_nft = await self.utils.get_confirmed_nft1155_tx_to_notify()
        if len(pending_nft) > 0:
            for each in pending_nft:
                try:
                    name = each['name']
                    eth_token = 0
                    matic_token = 0
                    amount = each['amount']
                    url = self.bot.config['endpoint'][each['network'].lower()]
                    if each['network'] == "ETHEREUM":
                        eth_token = each['amount']
                    elif each['network'] == "POLYGON":
                        matic_token = each['amount']
                    else:
                        continue

                    if each['amount'] > 1000:
                        print("Skipped tx {} on network: {} for amount token ID {}".format(
                            each['transaction_hash'], each['network'], each['amount'])
                        )
                        continue

                    crediting = (
                        each['network'], each['token_address'],
                        each['token_id_int'], each['token_id_hex'], each['discord_id'], 
                        each['user_server'], amount
                    )

                    check_tx = await eth_get_tx_info(url, each['transaction_hash'], 8)
                    status = "CONFIRMED"
                    if check_tx and 'status' in check_tx and int(check_tx['status'], 16) == 0:
                        status = "FAILED" # leave it there
                        continue # skip failed tx
                    else:
                        msg = "You have deposited `{}` of `{}` with tx `{}` from address: `{}` at height `{}` " \
                              "on network `{}`.".format(each['amount'], name, each['transaction_hash'],
                                                        each['from_address'], each['block_number'], each['network']
                                                        )
                        if each['user_server'] == self.bot.server_bot and each['discord_id'].isdigit():
                            found_user = self.bot.get_user(int(each['discord_id']))
                            if found_user:
                                try:
                                    await found_user.send(msg)
                                except Exception:
                                    traceback.print_exc(file=sys.stdout)
                                await self.utils.insert_nft_deposited_credits(
                                    each['discord_id'], each['user_server'], 
                                    eth_token, matic_token, crediting, each['nft_tx_id'])
                except Exception:
                    traceback.print_exc(file=sys.stdout)

    @tasks.loop(seconds=20.0)
    async def check_pending_withdraw_gas(self):
        await self.bot.wait_until_ready()
        pending_tx = await self.utils.get_pending_withdraw_tx_list_all()
        if len(pending_tx) > 0:
            for each in pending_tx:
                try:
                    url = self.bot.config['endpoint'][each['network'].lower()]
                    coin_decimal = self.bot.config['gas_decimal'][each['network'].lower()]
                    if each['network'] == "ETHEREUM":
                        coin = "ETH"
                    elif each['network'] == "POLYGON":
                        coin = "MATIC"
                    else:
                        continue
                    check_tx = await eth_get_tx_info(url, each['withdrew_tx'], 8)
                    status = "CONFIRMED"
                    if check_tx is None:
                        continue

                    if check_tx and 'status' in check_tx and int(check_tx['status'], 16) == 0:
                        status = "FAILED" # leave it there

                    real_tx_fee = int(check_tx['effectiveGasPrice'], 16)/10**coin_decimal * int(check_tx['gasUsed'], 16)
                    real_tx_fee = truncate(real_tx_fee, 18)
                    eth_gas = 0.0
                    matic_gas = 0.0
                    eth_nft_tx = 0
                    matic_nft_tx = 0
                    if each['network'] == "ETHEREUM":
                        eth_gas = real_tx_fee
                        eth_nft_tx = 1
                    elif each['network'] == "POLYGON":
                        matic_gas = real_tx_fee
                        matic_nft_tx = 1
                    await self.utils.update_withdraw_pending_tx(
                        each['withdraw_id'], each['withdrew_tx'], status,
                        int(check_tx['effectiveGasPrice'], 16), int(check_tx['gasUsed'], 16), real_tx_fee,
                        eth_gas, matic_gas, each['user_id'], each['user_server'],
                        eth_nft_tx, matic_nft_tx
                    )
                    msg = "Your withdraw of tx hash `{}` is *{}*. Fee {:,.6f} {}.".format(
                        each['withdrew_tx'], status, real_tx_fee, coin
                    )
                    if each['user_server'] == self.bot.server_bot and each['user_id'].isdigit():
                        found_user = self.bot.get_user(int(each['user_id']))
                        if found_user:
                            try:
                                await found_user.send(msg)
                            except Exception:
                                traceback.print_exc(file=sys.stdout)
                except Exception:
                    traceback.print_exc(file=sys.stdout)

    @tasks.loop(seconds=20.0)
    async def check_pending_erc1155_tx(self):
        await self.bot.wait_until_ready()
        pending_tx = await self.utils.get_nft_erc1155_unverified_tx()
        if len(pending_tx) > 0:
            # get list saved tx
            list_existing = await self.utils.get_nft_erc1155_list()
            list_existing_token_ids = [
                each['token_address'].lower() + each['token_id_hex'].lower() for each in list_existing
            ]
            data_rows = []
            for each in pending_tx:
                try:
                    if each['network'] == "ETHEREUM":
                        chain = "eth"
                    elif each['network'] == "POLYGON":
                        chain = "polygon"
                    else:
                        continue
                    # fetch from API
                    get_owners = await self.utils.nft_erc1155_get_nft_owners(
                        chain, each['token_address'], int(each['token_id_hex'], 16)
                    )
                    token_uri = None
                    metadata = None
                    name = None
                    thumb_url = None
                    if get_owners:
                        token_uri = get_owners[0]['token_uri']
                        metadata = get_owners[0]['metadata']
                        # collection_name = get_owners[0]['name']
                        name = json.loads(get_owners[0]['metadata'])['name']
                        thumb_url = json.loads(get_owners[0]['metadata'])['image']

                    if each['need_verify_token_id'] == 1:
                        if token_uri is None or metadata is None:
                            # print("{} {} Caught token_uri {} and metadata {}".format(each['network'],
                            # each['token_address'], token_uri,  metadata))
                            continue
                        is_verified = 0
                        if each['token_address'].lower() + each['token_id_hex'] not in list_existing_token_ids:
                            data_rows.append(
                                (each['network'], name, each['token_address'], each['token_id_hex'],
                                 token_uri, metadata, thumb_url, is_verified, int(time.time()), int(time.time()))
                            )
                    elif each['need_verify_token_id'] == 0:
                        # doesn't need verify, add to credit direct
                        # TODO: always add to meta in `table nft_item_list
                        pass
                    else:
                        continue
                except Exception:
                    traceback.print_exc(file=sys.stdout)
            if len(data_rows) > 0:
                await self.utils.nft_erc1155_new_to_list(data_rows)

    @commands.Cog.listener()
    async def on_ready(self):
        if not self.check_notify_deposit_gas.is_running():
            self.check_notify_deposit_gas.start()
        if not self.check_notify_deposit_nft.is_running():
            self.check_notify_deposit_nft.start()
        if not self.check_pending_withdraw_gas.is_running():
            self.check_pending_withdraw_gas.start()
        # check pending erc1155
        if not self.check_pending_erc1155_tx.is_running():
            self.check_pending_erc1155_tx.start()

    async def cog_load(self) -> None:
        if not self.check_notify_deposit_gas.is_running():
            self.check_notify_deposit_gas.start()
        if not self.check_notify_deposit_nft.is_running():
            self.check_notify_deposit_nft.start()
        if not self.check_pending_withdraw_gas.is_running():
            self.check_pending_withdraw_gas.start()
        # check pending erc1155
        if not self.check_pending_erc1155_tx.is_running():
            self.check_pending_erc1155_tx.start()

    async def cog_unload(self) -> None:
        self.check_notify_deposit_gas.cancel()
        self.check_notify_deposit_nft.cancel()
        self.check_pending_withdraw_gas.cancel()
        self.check_pending_erc1155_tx.cancel()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Deposit(bot))

