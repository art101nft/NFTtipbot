import asyncio
import json
import sys
import traceback
from datetime import datetime, timezone
import math

import aiohttp
from discord.ext import commands, tasks

from cogs.utils import Utils


# Cog class
class MoralisAPI(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.utils = Utils(self.bot)

    async def pull_nft_contract_meta(
        self, chain: str, contract: str, token_id_int: int, retry: int=5, timeout: int=45
    ):
        try:
            retrying = 0
            # https://deep-index.moralis.io/api/v2/nft/0x5846728730366d686cdc95dae80a70b44ec9eab2/0?chain=polygon&format=decimal

            url = self.bot.config['api']['moralis_tx_api_url'] + "nft/"+contract+"/"+str(token_id_int)+"?chain="+chain+"&format=decimal"
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers={'Content-Type': 'application/json', 'X-API-Key': self.bot.config['api']['moralis']}, 
                    timeout=timeout
                ) as response:
                    if response.status == 200:
                        res_data = await response.read()
                        res_data = res_data.decode('utf-8')
                        await session.close()
                        decoded_data = json.loads(res_data)
                        if decoded_data and 'metadata' in decoded_data and decoded_data['metadata']:
                            return decoded_data
                    else:
                        # print("pull_nft_contract_transfers chain {}, contract {} got status {}".format(chain, contract, response.status))
                        retrying += 1
                        while retrying < retry:
                            await asyncio.sleep(10.0)
                            async with aiohttp.ClientSession() as session:
                                async with session.get(
                                    url,
                                    headers={'Content-Type': 'application/json', 'X-API-Key': self.bot.config['api']['moralis']}, 
                                    timeout=timeout
                                ) as response:
                                    if response.status == 200:
                                        res_data = await response.read()
                                        res_data = res_data.decode('utf-8')
                                        await session.close()
                                        decoded_data = json.loads(res_data)
                                        if decoded_data and 'metadata' in decoded_data and decoded_data['metadata']:
                                            return decoded_data
                                    else:
                                        retrying += 1
        except asyncio.TimeoutError:
            print('TIMEOUT: pull_nft_contract_meta chain {}, contract {}, token id {}'.format(chain, contract, token_id_int))
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        return None

    async def pull_nft_contract_transfers(self, chain: str, contract: str, cursor: str=None, timeout: int=45):
        """
        if has_data is True, we run default
        else, we will loop until the last page and insert
        """
        try:
            retry = 5
            retrying = 0
            url = self.bot.config['api']['moralis_tx_api_url'] + "nft/"+contract+"/transfers?chain="+\
                chain+"&format=decimal&limit=100"
            if cursor:
                url = self.bot.config['api']['moralis_tx_api_url'] + "nft/"+contract+"/transfers?chain="+\
                    chain+"&format=decimal&limit=100&cursor="+cursor
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers={'Content-Type': 'application/json', 'X-API-Key': self.bot.config['api']['moralis']}, 
                    timeout=timeout
                ) as response:
                    if response.status == 200:
                        res_data = await response.read()
                        res_data = res_data.decode('utf-8')
                        await session.close()
                        decoded_data = json.loads(res_data)
                        if decoded_data and 'result' in decoded_data and decoded_data['result']:
                            return decoded_data
                    else:
                        # print("pull_nft_contract_transfers chain {}, contract {} got status {}".format(chain, contract, response.status))
                        retrying += 1
                        while retrying < retry:
                            await asyncio.sleep(10.0)
                            async with aiohttp.ClientSession() as session:
                                async with session.get(
                                    url,
                                    headers={'Content-Type': 'application/json', 'X-API-Key': self.bot.config['api']['moralis']}, 
                                    timeout=timeout
                                ) as response:
                                    if response.status == 200:
                                        res_data = await response.read()
                                        res_data = res_data.decode('utf-8')
                                        await session.close()
                                        decoded_data = json.loads(res_data)
                                        if decoded_data and 'result' in decoded_data and decoded_data['result']:
                                            return decoded_data
                                    else:
                                        retrying += 1
        except asyncio.TimeoutError:
            print('TIMEOUT: pull_nft_contract_transfers chain {}, contract {}'.format(chain, contract))
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        return None

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
                                            data_rows.append((
                                                network, each['hash'], int(each['nonce']),
                                                int(each['transaction_index']),
                                                each['from_address'], each['to_address'], int(each['value']),
                                                int(each['gas']), int(each['gas_price']), each['input'],
                                                int(each['receipt_cumulative_gas_used']),
                                                int(each['receipt_gas_used']),
                                                int(each['receipt_status']), each['block_timestamp'], int(timestamp),
                                                int(each['block_number']), each['block_hash']
                                            ))
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
                                            data_rows.append((
                                                network, int(each['block_number']), each['block_timestamp'], int(timestamp),
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
    async def fetch_nft_contract_meta(self):
        time_sleep_each = 20.0
        list_meta_contracts = await self.utils.get_nft_contract_meta_tracking()
        if len(list_meta_contracts) > 0:
            for each in list_meta_contracts:
                if each['network'] == "POLYGON":
                    chain = "polygon"
                elif each['network'] == "ETHEREUM":
                    chain = "eth"
                try:
                    # collected token id from transferred database
                    get_token_ids = await self.utils.get_nft_contract_transferred_token_ids(each['contract'])
                    # if there is any token id, meta already saved
                    get_saved_meta_ids = await self.utils.get_nft_contract_meta_list(each['nft_cont_tracking'])
                    if len(get_token_ids) > 0 and len(get_saved_meta_ids) == len(get_token_ids):
                        print("Chain: {} Contract {} collected data {}. Should update `need_update_meta`=0".format(
                            chain, each['contract'], len(get_token_ids))
                        )
                    elif len(get_token_ids) > 0 and len(get_saved_meta_ids) < len(get_token_ids):
                        print("Chain: {} Contract {} collected data {}. Still have meta {} more to update".format(
                            chain, each['contract'], len(get_token_ids), len(get_token_ids) - len(get_saved_meta_ids))
                        )
                        # collect meta
                        data_rows = []
                        existing_saved_token_ids = []
                        if len(get_saved_meta_ids) > 0:
                            existing_saved_token_ids = [int(each['token_id_hex'], 16) for each in get_saved_meta_ids]
                        for token_id_each in get_token_ids:
                            try:
                                if int(token_id_each['token_id_hex'], 16) not in existing_saved_token_ids:
                                    fetch_each_meta = await self.pull_nft_contract_meta(
                                        chain, each['contract'], int(token_id_each['token_id_hex'], 16), 5, 45
                                    )
                                    await asyncio.sleep(0.5)
                                    if fetch_each_meta:
                                        last_token_uri_sync_time=datetime.strptime(
                                            fetch_each_meta['last_token_uri_sync'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                        ).replace(tzinfo=timezone.utc).timestamp()

                                        last_metadata_sync_time=datetime.strptime(
                                            fetch_each_meta['last_metadata_sync'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                        ).replace(tzinfo=timezone.utc).timestamp()
                                        data_rows.append((
                                            each['nft_cont_tracking'], token_id_each['token_id_int'], token_id_each['token_id_hex'],
                                            fetch_each_meta['token_uri'], fetch_each_meta['metadata'],
                                            fetch_each_meta['last_token_uri_sync'], last_token_uri_sync_time, 
                                            fetch_each_meta['last_metadata_sync'], last_metadata_sync_time
                                        ))
                            except Exception as e:
                                traceback.print_exc(file=sys.stdout)
                            if len(data_rows) > 0 and len(data_rows) % 10 == 0:
                                print("Fetch Meta Chain: {} Contract {} fetched {}. Still have {} more to update".format(
                                    chain, each['contract'], len(data_rows), len(get_token_ids) - len(data_rows))
                                )
                        if len(data_rows) > 0:
                            inserting = await self.utils.insert_nft_contract_meta(data_rows)
                            print("Fetch Meta Chain: {} Contract {} fetched {}. inserted {}".format(
                                chain, each['contract'], len(data_rows), inserting)
                            )
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)

    @tasks.loop(seconds=60.0)
    async def fetch_nft_contract_transfers(self):
        time_sleep = 20.0
        # get list of tracking contract, fetch one by one
        list_tracking_contracts = await self.utils.get_nft_contract_transfer_tracking()
        if len(list_tracking_contracts) > 0:
            for each in list_tracking_contracts:
                if each['network'] == "POLYGON":
                    chain = "polygon"
                elif each['network'] == "ETHEREUM":
                    chain = "eth"
                try:
                    # check if there is data
                    having_data_rows = []
                    get_existing_contract_tx = await self.utils.get_nft_each_contract_transfers(each['contract'], each['network'])
                    if len(get_existing_contract_tx) == 0:
                        get_new_nft_tx = await self.pull_nft_contract_transfers(chain, each['contract'], None, 45)
                        await asyncio.sleep(time_sleep)
                        if get_new_nft_tx is None:
                            continue
                        got_total_nft = get_new_nft_tx['total']
                        if get_new_nft_tx:
                            if len(get_new_nft_tx['result']) > 0:
                                having_data_rows = having_data_rows + get_new_nft_tx['result']
                            if get_new_nft_tx['total'] > 100:
                                # loop
                                total_page = math.ceil(get_new_nft_tx['total'] / 100)
                                cursor = get_new_nft_tx['cursor']
                                for i in range(1, total_page):
                                    # print("Cursor + {} / Contract: {}, chain: {}".format(i, each['contract'], chain))
                                    try:
                                        get_new_nft_tx = await self.pull_nft_contract_transfers(chain, each['contract'], cursor, 45)
                                        if get_new_nft_tx is None:
                                            print("Got none for chain: {}, contract: {} at cursor={} ".format(chain, each['contract'], cursor))
                                            break
                                        cursor = get_new_nft_tx['cursor']
                                        if get_new_nft_tx and len(get_new_nft_tx['result']) > 0:
                                            having_data_rows = having_data_rows + get_new_nft_tx['result']
                                            print("Chain: {}, contract {}, total: {} vs having {} - up timestamp {}".format(
                                                chain, each['contract'], got_total_nft, len(having_data_rows), get_new_nft_tx['result'][-1]['block_timestamp']
                                            ))
                                    except Exception as e:
                                        traceback.print_exc(file=sys.stdout)
                                        break
                                    await asyncio.sleep(time_sleep)
                        if got_total_nft > len(having_data_rows):
                            print("Chain: {}, contract {}, Mis-match records, total: {} vs having {}".format(
                                chain, each['contract'], got_total_nft, len(having_data_rows)
                            ))
                        else:
                            # Before insert
                            print("Chain: {}, contract {}, records, total: {} vs having {}".format(
                                chain, each['contract'], got_total_nft, len(having_data_rows)
                            ))
                            data_rows = []
                            existing_pending = []
                            for item in having_data_rows:
                                if "{}_{}_{}_{}".format(
                                    item['transaction_hash'].lower(), item['token_id'], item['from_address'].lower(), item['to_address'].lower()
                                ) in existing_pending:
                                    continue
                                else:
                                    existing_pending.append("{}_{}_{}_{}".format(
                                        item['transaction_hash'].lower(), item['token_id'], item['from_address'].lower(), item['to_address'].lower()
                                    ))

                                token_id_int = int(item['token_id']) if int(item['token_id']) < 10**20 else None
                                timestamp=datetime.strptime(
                                    item['block_timestamp'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                ).replace(tzinfo=timezone.utc).timestamp()
                                data_rows.append((
                                    each['network'], int(item['block_number']), item['block_timestamp'],
                                    timestamp, item['block_hash'], item['transaction_hash'],
                                    item['transaction_index'], item['log_index'], item['value'],
                                    item['contract_type'], item['transaction_type'], item['token_address'],
                                    token_id_int, hex(int(item['token_id'])), item['from_address'],
                                    item['to_address'], int(item['amount']), item['verified']
                                ))
                            if len(data_rows) > 0:
                                inserting = await self.utils.insert_nft_contract_transfers(data_rows)
                                print("NFT Contract {}, chain {} transfer inserted: {} vs data_rows: {}".format(
                                    each['contract'], chain, inserting, len(data_rows)))
                    else:
                        # Have data
                        get_new_nft_tx = await self.pull_nft_contract_transfers(chain, each['contract'], None, 45)
                        existing_tx = ["{}_{}_{}_{}".format(
                            each['transaction_hash'].lower(), each['token_id_int'], each['from_address'].lower(), 
                            each['to_address'].lower()) for each in get_existing_contract_tx
                        ]
                        await asyncio.sleep(time_sleep)
                        if get_new_nft_tx and len(get_new_nft_tx['result']) > 0:
                            data_rows = []
                            for item in get_new_nft_tx['result']:
                                if "{}_{}_{}_{}".format(
                                    item['transaction_hash'].lower(), item['token_id'], item['from_address'].lower(), item['to_address'].lower()
                                ) not in existing_tx:
                                    token_id_int = int(item['token_id']) if int(item['token_id']) < 10**20 else None
                                    timestamp=datetime.strptime(
                                        item['block_timestamp'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                    ).replace(tzinfo=timezone.utc).timestamp()
                                    data_rows.append((
                                        each['network'], int(item['block_number']), item['block_timestamp'],
                                        timestamp, item['block_hash'], item['transaction_hash'],
                                        item['transaction_index'], item['log_index'], item['value'],
                                        item['contract_type'], item['transaction_type'], item['token_address'],
                                        token_id_int, hex(int(item['token_id'])), item['from_address'],
                                        item['to_address'], int(item['amount']), item['verified']
                                    ))
                            if len(data_rows) > 0:
                                inserting = await self.utils.insert_nft_contract_transfers(data_rows)
                                print("NFT Contract {}, chain {} transfer inserted: {} / data_rows: {}".format(
                                    each['contract'], chain, inserting, len(data_rows)))
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
        if not self.fetch_nft_contract_meta.is_running():
            self.fetch_nft_contract_meta.start()
        if not self.fetch_nft_contract_transfers.is_running():
            self.fetch_nft_contract_transfers.start()
        if not self.wallet_eth.is_running():
            self.wallet_eth.start()
        if not self.wallet_nft_eth.is_running():
            self.wallet_nft_eth.start()
        if not self.wallet_polygon.is_running():
            self.wallet_polygon.start()
        if not self.wallet_nft_polygon.is_running():
            self.wallet_nft_polygon.start()

    async def cog_load(self) -> None:
        if not self.fetch_nft_contract_meta.is_running():
            self.fetch_nft_contract_meta.start()
        if not self.fetch_nft_contract_transfers.is_running():
            self.fetch_nft_contract_transfers.start()
        if not self.wallet_eth.is_running():
            self.wallet_eth.start()
        if not self.wallet_nft_eth.is_running():
            self.wallet_nft_eth.start()
        if not self.wallet_polygon.is_running():
            self.wallet_polygon.start()
        if not self.wallet_nft_polygon.is_running():
            self.wallet_nft_polygon.start()

    async def cog_unload(self) -> None:
        self.fetch_nft_contract_meta.cancel()
        self.fetch_nft_contract_transfers.cancel()
        self.wallet_eth.cancel()
        self.wallet_nft_eth.cancel()
        self.wallet_polygon.cancel()
        self.wallet_nft_polygon.cancel()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MoralisAPI(bot))