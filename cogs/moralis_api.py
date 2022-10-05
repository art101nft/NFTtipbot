import asyncio
import json
import sys
import traceback
from datetime import datetime, timezone

import aiohttp
from discord.ext import commands, tasks

from cogs.utils import Utils


# Cog class
class MoralisAPI(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.utils = Utils(self.bot)

    async def pull_wallet_tx(self, chain: str):
        try:
            url = self.bot.config['api']['moralis_tx_api_url'] + self.bot.config['wallet']['eth_address'] + "?chain=" + chain
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers={'Content-Type': 'application/json', 'X-API-Key': self.bot.config['api']['moralis']}, 
                    timeout=30
                ) as response:
                    if response.status == 200:
                        res_data = await response.read()
                        res_data = res_data.decode('utf-8')
                        await session.close()
                        decoded_data = json.loads(res_data)
                        if decoded_data and 'result' in decoded_data and decoded_data['result'] \
                                and decoded_data['result']:
                            # get existing tx
                            list_existings = await self.utils.get_wallet_tx(200)
                            tx_existing_list = [each['hash'].lower() for each in list_existings]
                            # OK, we have result
                            data_rows = []
                            if chain == "eth":
                                network = "ETHEREUM"
                            elif chain == "polygon":
                                network = "POLYGON"
                            elif chain == "fantom":
                                network = "FANTOM" 
                            if len(decoded_data['result']) > 0:
                                for each in decoded_data['result']:
                                    try:
                                        if each['hash'].lower() not in tx_existing_list:
                                            timestamp=datetime.strptime(
                                                each['block_timestamp'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                            ).replace(tzinfo=timezone.utc).timestamp()
                                            data_rows.append(
                                                (
                                                    network, each['hash'], int(each['nonce']),
                                                    int(each['transaction_index']),
                                                    each['from_address'], each['to_address'], int(each['value']),
                                                    int(each['gas']), int(each['gas_price']), each['input'],
                                                    int(each['receipt_cumulative_gas_used']),
                                                    int(each['receipt_gas_used']),
                                                    int(each['receipt_status']), each['block_timestamp'], int(timestamp),
                                                    int(each['block_number']), each['block_hash']
                                                )
                                            )
                                    except Exception as e:
                                        traceback.print_exc(file=sys.stdout)
                                if len(data_rows) > 0:
                                    records = await self.utils.get_insert_wallet_txs(data_rows)
                                    msg = f"pull_wallet_tx chain {chain}: data_rows {str(len(data_rows))} and records: {str(records)}"
                                    print(msg)
                                    await self.utils.log_to_channel(
                                        self.bot.config['discord']['log_channel'], 
                                        msg
                                    )
        except asyncio.TimeoutError:
            print('TIMEOUT: pull_wallet_tx chain {}'.format(chain))
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
    
    async def pull_nft_tx(self, chain: str):
        try:
            url = self.bot.config['api']['moralis_nft_tx_api_url'] + self.bot.config['wallet']['eth_address'] + \
                  "/nft/transfers?chain="+chain+"&format=decimal&direction=both"
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        url,
                        headers={'Content-Type': 'application/json', 'X-API-Key': self.bot.config['api']['moralis']},
                        timeout=30
                ) as response:
                    if response.status == 200:
                        res_data = await response.read()
                        res_data = res_data.decode('utf-8')
                        await session.close()
                        decoded_data = json.loads(res_data)
                        if decoded_data and 'result' in decoded_data and \
                                decoded_data['result'] and decoded_data['result']:
                            # get existing tx
                            list_existing = await self.utils.get_wallet_nft_tx(200)
                            tx_existing_list = [
                                t['transaction_hash'].lower() + t['token_address'].lower() for t in list_existing
                            ]
                            # OK, we have result
                            data_rows = []
                            if chain == "eth":
                                network = "ETHEREUM"
                            elif chain == "polygon":
                                network = "POLYGON"
                            elif chain == "fantom":
                                network = "FANTOM"
                            if len(decoded_data['result']) > 0:
                                for each in decoded_data['result']:
                                    try:
                                        token_id_int = int(each['token_id']) if int(each['token_id']) < 10**20 else None
                                        if each['transaction_hash'].lower() + each['token_address'].lower() not in tx_existing_list:
                                            timestamp = datetime.strptime(
                                                each['block_timestamp'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                            ).replace(tzinfo=timezone.utc).timestamp()
                                            data_rows.append((network, int(each['block_number']), each['block_timestamp'], int(timestamp),
                                                              each['block_hash'], each['transaction_hash'], each['transaction_index'],
                                                              each['log_index'], each['value'], each['contract_type'], each['transaction_type'],
                                                              each['token_address'], token_id_int, hex(int(each['token_id'])),
                                                              each['from_address'], each['to_address'], int(each['amount']),
                                                              each['verified']
                                                            ))
                                    except Exception as e:
                                        traceback.print_exc(file=sys.stdout)
                                if len(data_rows) > 0:
                                    records = await self.utils.get_insert_wallet_nft_txs(data_rows)
                                    msg = f"pull_nft_tx chain {chain}: data_rows {str(len(data_rows))} and records: {str(records)}"
                                    print(msg)
                                    await self.utils.log_to_channel(
                                        self.bot.config['discord']['log_channel'], 
                                        msg
                                    )
        except asyncio.TimeoutError:
            print('TIMEOUT: pull_nft_tx chain {}'.format(chain))
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    @tasks.loop(seconds=60.0)
    async def wallet_eth(self):
        try:
            await self.pull_wallet_tx("eth")
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    @tasks.loop(seconds=40.0)
    async def wallet_nft_eth(self):
        try:
            await self.pull_nft_tx("eth")
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    @tasks.loop(seconds=60.0)
    async def wallet_polygon(self):
        try:
            await self.pull_wallet_tx("polygon")
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    @tasks.loop(seconds=40.0)
    async def wallet_nft_polygon(self):
        try:
            await self.pull_nft_tx("polygon")
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    @tasks.loop(seconds=65.0)
    async def wallet_fantom(self):
        try:
            await self.pull_wallet_tx("fantom")
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    @tasks.loop(seconds=70.0)
    async def wallet_nft_fantom(self):
        try:
            await self.pull_nft_tx("fantom")
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
    
    @commands.Cog.listener()
    async def on_ready(self):
        if not self.wallet_eth.is_running():
            self.wallet_eth.start()
        if not self.wallet_nft_eth.is_running():
            self.wallet_nft_eth.start()
        if not self.wallet_polygon.is_running():
            self.wallet_polygon.start()
        if not self.wallet_nft_polygon.is_running():
            self.wallet_nft_polygon.start()

    async def cog_load(self) -> None:
        if not self.wallet_eth.is_running():
            self.wallet_eth.start()
        if not self.wallet_nft_eth.is_running():
            self.wallet_nft_eth.start()
        if not self.wallet_polygon.is_running():
            self.wallet_polygon.start()
        if not self.wallet_nft_polygon.is_running():
            self.wallet_nft_polygon.start()

    async def cog_unload(self) -> None:
        self.wallet_eth.cancel()
        self.wallet_nft_eth.cancel()
        self.wallet_polygon.cancel()
        self.wallet_nft_polygon.cancel()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MoralisAPI(bot))