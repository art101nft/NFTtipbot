import asyncio
import json
import sys
import traceback
from datetime import datetime, timezone
import math
import time
import random
import io
import magic
from PIL import Image
import hashlib

import aiohttp
from discord.ext import commands, tasks

from cogs.utils import Utils


# Cog class
class MoralisAPI(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.utils = Utils(self.bot)

    async def push_resync_meta(
        self, chain: str, contract: str, token_id_int: int, timeout: int = 8
    ):
        url = self.bot.config['api']['moralis_tx_api_url'] + "nft/"+contract+"/"+str(token_id_int)+"/metadata/resync?chain="+chain+"&flag=uri&mode=async"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers={'Content-Type': 'application/json', 'X-API-Key': self.bot.config['api']['moralis']}, 
                    timeout=timeout
                ) as response:
                    if response.status == 200:
                        return True
                    elif response.status == 429:
                        await asyncio.sleep(30.0)
        except asyncio.TimeoutError:
            print('TIMEOUT: push_resync_meta chain {}, contract {}, token id: {}'.format(chain, contract, token_id_int))
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        return False

    async def pull_nft_contract_meta_list(
        self, chain: str, contract: str, retry: int=5, cursor: str=None, timeout: int=45
    ):
        try:
            retrying = 0
            url = self.bot.config['api']['moralis_tx_api_url'] + "nft/"+contract+"?chain="+chain+"&format=decimal"
            if cursor:
                url += "&cursor="+cursor
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
            print('TIMEOUT: pull_nft_contract_meta_list chain {}, contract {}'.format(chain, contract))
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        return None

    async def pull_nft_contract_meta(
        self, chain: str, contract: str, token_id_int: int, retry: int=5, timeout: int=45
    ):
        try:
            retrying = 0
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

    async def pull_nft_contract_transfers(
        self, chain: str, contract: str, cursor: str=None, timeout: int=45
    ):
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
                    data_rows = []
                    having_data_rows = []
                    if len(get_saved_meta_ids) == 0 and len(get_token_ids) > 0:
                        # In case no data, we fetch them all first
                        get_new_nft_meta = await self.pull_nft_contract_meta_list(chain, each['contract'], 5, None, 45)
                        await asyncio.sleep(time_sleep_each)
                        if get_new_nft_meta is None:
                            continue
                        got_total_meta = get_new_nft_meta['total']
                        if get_new_nft_meta:
                            if len(get_new_nft_meta['result']) > 0:
                                having_data_rows = having_data_rows + get_new_nft_meta['result']
                            if get_new_nft_meta['total'] > 100:
                                # loop
                                total_page = math.ceil(get_new_nft_meta['total'] / 100)
                                cursor = get_new_nft_meta['cursor']
                                for i in range(1, total_page):
                                    print("Meta Cursor + {} / Contract: {}, chain: {}".format(i, each['contract'], chain))
                                    try:
                                        get_new_nft_meta = await self.pull_nft_contract_meta_list(chain, each['contract'], 5, cursor, 45)
                                        if get_new_nft_meta is None:
                                            print("Meta Got none for chain: {}, contract: {} at cursor={} ".format(chain, each['contract'], cursor))
                                            break
                                        cursor = get_new_nft_meta['cursor']
                                        if get_new_nft_meta and len(get_new_nft_meta['result']) > 0:
                                            having_data_rows = having_data_rows + get_new_nft_meta['result']
                                            print("Chain: {}, contract {}, total: {} vs having {} - last_token_uri_sync {}".format(
                                                chain, each['contract'], got_total_meta, len(having_data_rows), get_new_nft_meta['result'][-1]['last_token_uri_sync']
                                            ))
                                    except Exception as e:
                                        traceback.print_exc(file=sys.stdout)
                                        break
                                    await asyncio.sleep(time_sleep_each)
                        if len(having_data_rows) > 0:
                            for item in having_data_rows:
                                try:
                                    last_token_uri_sync_time = None
                                    if item['last_token_uri_sync']:
                                        last_token_uri_sync_time=datetime.strptime(
                                            item['last_token_uri_sync'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                        ).replace(tzinfo=timezone.utc).timestamp()

                                    last_metadata_sync_time = None
                                    if item['last_metadata_sync']:
                                        last_metadata_sync_time=datetime.strptime(
                                            item['last_metadata_sync'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                        ).replace(tzinfo=timezone.utc).timestamp()
                                    token_id_int = int(item['token_id']) if int(item['token_id']) < 10**20 else None
                                    token_id_hex = hex(int(item['token_id']))
                                    data_rows.append((
                                        each['nft_cont_tracking'], token_id_int, token_id_hex,
                                        item['token_uri'], item['metadata'],
                                        item['last_token_uri_sync'], last_token_uri_sync_time, 
                                        item['last_metadata_sync'], last_metadata_sync_time
                                    ))
                                except Exception as e:
                                    traceback.print_exc(file=sys.stdout)
                            if len(data_rows) > 0:
                                inserting = await self.utils.insert_nft_contract_meta(data_rows)
                                print("Fetch Meta Chain: {} Contract {} fetched {}. inserted {}".format(
                                    chain, each['contract'], len(data_rows), inserting)
                                )
                    elif len(get_token_ids) > 0 and len(get_saved_meta_ids) == len(get_token_ids):
                        print("Chain: {} Contract {} collected data {}. Should update `need_update_meta`=0".format(
                            chain, each['contract'], len(get_token_ids))
                        )
                    elif len(get_token_ids) > 0 and len(get_saved_meta_ids) < len(get_token_ids):
                        remaining_token_ids = len(get_token_ids) - len(get_saved_meta_ids) 
                        failed_fetch_transfer = 0
                        print("Chain: {} Contract {} collected data {}. Still have meta {} more to update".format(
                            chain, each['contract'], len(get_token_ids), len(get_token_ids) - len(get_saved_meta_ids))
                        )
                        # collect meta
                        existing_saved_token_ids = []
                        if len(get_saved_meta_ids) > 0:
                            existing_saved_token_ids = [int(each['token_id_hex'], 16) for each in get_saved_meta_ids]
                            for token_id_each in get_token_ids:
                                try:
                                    if int(token_id_each['token_id_hex'], 16) not in existing_saved_token_ids:
                                        if len(get_saved_meta_ids) > 0:
                                            # Just to print what additional data to get?
                                            print("pulling fetch chain: {}, contract: {}, token id: {}".format(
                                                chain, each['contract'], int(token_id_each['token_id_hex'], 16)
                                            ))
                                        fetch_each_meta = await self.pull_nft_contract_meta(
                                            chain, each['contract'], int(token_id_each['token_id_hex'], 16), 5, 45
                                        )
                                        await asyncio.sleep(0.1)
                                        if fetch_each_meta:
                                            last_token_uri_sync_time=None
                                            if fetch_each_meta['last_token_uri_sync']:
                                                last_token_uri_sync_time=datetime.strptime(
                                                    fetch_each_meta['last_token_uri_sync'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                                ).replace(tzinfo=timezone.utc).timestamp()

                                            last_metadata_sync_time=None
                                            if fetch_each_meta['last_metadata_sync']:
                                                last_metadata_sync_time=datetime.strptime(
                                                    fetch_each_meta['last_metadata_sync'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                                ).replace(tzinfo=timezone.utc).timestamp()
                                            data_rows.append((
                                                each['nft_cont_tracking'], token_id_each['token_id_int'], token_id_each['token_id_hex'],
                                                fetch_each_meta['token_uri'], fetch_each_meta['metadata'],
                                                fetch_each_meta['last_token_uri_sync'], last_token_uri_sync_time, 
                                                fetch_each_meta['last_metadata_sync'], last_metadata_sync_time
                                            ))
                                            remaining_token_ids -= 1
                                        else:
                                            failed_fetch_transfer += 1
                                            if failed_fetch_transfer > 0 and failed_fetch_transfer % 5 == 0:
                                                print("Fetch Meta Chain: {} Contract {} failed {} times".format(
                                                    chain, each['contract'], failed_fetch_transfer)
                                                )
                                            await asyncio.sleep(30.0)
                                except Exception as e:
                                    traceback.print_exc(file=sys.stdout)
                                if len(data_rows) > 0 and len(data_rows) % 5 == 0:
                                    print("Fetch Meta Chain: {} Contract {} fetched {}. Still have {} more to update".format(
                                        chain, each['contract'], len(data_rows), remaining_token_ids)
                                    )
                                    inserting = await self.utils.insert_nft_contract_meta(data_rows)
                                    print("Fetch Meta Chain: {} Contract {} fetched {}. inserted {}".format(
                                        chain, each['contract'], len(data_rows), inserting)
                                    )
                                    data_rows = [] 
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
                                            print("Nft Got none for chain: {}, contract: {} at cursor={} ".format(chain, each['contract'], cursor))
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
    async def fetch_nft_meta_syncing(self):
        get_unsync_list = await self.utils.get_nft_contract_unsync_meta_list()
        meta_updates = []
        if len(get_unsync_list) > 0:
            failed_push_resync_meta = 0
            for each in get_unsync_list:
                try:
                    if each['network'] == "POLYGON":
                        chain = "polygon"
                    elif each['network'] == "ETHEREUM":
                        chain = "eth"

                    if each['token_uri']:
                        # has URI but no meta, fetch Meta
                        try:
                            token_uri_url = each['token_uri']
                            if token_uri_url.startswith("https://ipfs.moralis.io:2053/ipfs/"):
                                token_uri_url = random.choice(self.bot.config['ipfs_gateway']['public']) + \
                                    token_uri_url.replace("https://ipfs.moralis.io:2053/ipfs/", "")

                            async with aiohttp.ClientSession() as session:
                                async with session.get(
                                    token_uri_url,
                                    headers={'Content-Type': 'application/json'}, 
                                    timeout=16
                                ) as response:
                                    if response.status == 200:
                                        res_data = await response.read()
                                        res_data = res_data.decode('utf-8')
                                        await session.close()
                                        decoded_data = json.loads(res_data)
                                        if each['last_token_uri_sync']:
                                            last_token_uri_sync = each['last_token_uri_sync']
                                            last_token_uri_sync_time = each['last_token_uri_sync_time']
                                            last_metadata_sync_time = int(time.time())
                                        meta_updates.append((
                                            res_data, # change from decoded_data
                                            each['token_uri'],
                                            last_token_uri_sync,
                                            last_token_uri_sync_time,
                                            datetime.utcfromtimestamp(int(time.time())).strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                                            last_metadata_sync_time,
                                            each['nft_cont_meta_id']
                                        ))
                        except aiohttp.client_exceptions.InvalidURL:
                            print("Fetch invalid token_uri url: {}".format(each['token_uri']))
                        except asyncio.exceptions.TimeoutError:
                            print("Fetch token_uri timeout for url: {}".format(each['token_uri']))
                        except Exception as e:
                            traceback.print_exc(file=sys.stdout)
                    elif each['token_uri'] is None:
                        # Check if there is a call before
                        if (each['pushing_updated'] and int(time.time()) - each['pushing_updated'] > 300) or \
                            (each['token_uri'] is None and each['metadata']):
                            get_new_meta = await self.pull_nft_contract_meta(
                                chain, each['contract'], int(each['token_id_hex'], 16), 3, 8
                            )
                            if get_new_meta and get_new_meta['token_uri'] and get_new_meta['metadata']:
                                last_token_uri_sync_time = None
                                if get_new_meta['last_token_uri_sync']:
                                    last_token_uri_sync_time=datetime.strptime(
                                        get_new_meta['last_token_uri_sync'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                    ).replace(tzinfo=timezone.utc).timestamp()

                                last_metadata_sync_time = None
                                if get_new_meta['last_metadata_sync']:
                                    last_metadata_sync_time=datetime.strptime(
                                        get_new_meta['last_metadata_sync'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                    ).replace(tzinfo=timezone.utc).timestamp()
                                meta_updates.append((
                                    get_new_meta['metadata'],
                                    get_new_meta['token_uri'],
                                    get_new_meta['last_token_uri_sync'],
                                    last_token_uri_sync_time,
                                    get_new_meta['last_metadata_sync'],
                                    last_metadata_sync_time,
                                    each['nft_cont_meta_id']
                                ))
                        elif each['pushing_updated'] is None:
                            # Try to check it first if we can get data, otherwise, call resync
                            get_new_meta = await self.pull_nft_contract_meta(
                                chain, each['contract'], int(each['token_id_hex'], 16), 3, 8
                            )
                            if get_new_meta and get_new_meta['token_uri'] and get_new_meta['metadata']:
                                last_token_uri_sync_time = None
                                if get_new_meta['last_token_uri_sync']:
                                    last_token_uri_sync_time=datetime.strptime(
                                        get_new_meta['last_token_uri_sync'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                    ).replace(tzinfo=timezone.utc).timestamp()

                                last_metadata_sync_time = None
                                if get_new_meta['last_metadata_sync']:
                                    last_metadata_sync_time=datetime.strptime(
                                        get_new_meta['last_metadata_sync'].split(".")[0]+"Z", '%Y-%m-%dT%H:%M:%SZ'
                                    ).replace(tzinfo=timezone.utc).timestamp()
                                meta_updates.append((
                                    get_new_meta['metadata'],
                                    get_new_meta['token_uri'],
                                    get_new_meta['last_token_uri_sync'],
                                    last_token_uri_sync_time,
                                    get_new_meta['last_metadata_sync'],
                                    last_metadata_sync_time,
                                    each['nft_cont_meta_id']
                                ))
                            else:
                                # Call API to resync
                                push_resync = await self.push_resync_meta(chain, each['contract'], int(each['token_id_hex'], 16), 8)
                                if push_resync is True:
                                    await self.utils.update_nft_tracking_meta_called_update(
                                        each['nft_cont_meta_id']
                                    )
                                    print("Pushing meta to resync chain: {}, contract: {}, token id: {}".format(
                                        chain, each['contract'], int(each['token_id_hex'], 16)
                                    ))
                                else:
                                    failed_push_resync_meta += 1
                                    if failed_push_resync_meta % 10 == 0:
                                        print("Pushing meta to resync chain: {}, contract: {}, failed {} times.".format(
                                            chain, each['contract'], failed_push_resync_meta
                                        ))
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
                if len(meta_updates) > 0 and len(meta_updates) % 5 == 0:
                    updating = await self.utils.update_nft_tracking_meta(meta_updates)
                    print("Updated meta chain: {}, contract: {}, updated: {}".format(
                        chain, each['contract'], updating
                    ))
                    meta_updates = []

            if len(meta_updates) > 0:
                updating = await self.utils.update_nft_tracking_meta(meta_updates)
                print("Updated meta chain: {}, contract: {}, updated: {}".format(
                    chain, each['contract'], updating
                ))

    @tasks.loop(seconds=60.0)
    async def fetch_image_in_nft_cont_meta(self):
        get_list_no_images = await self.utils.get_nft_contract_meta_list_no_image()
        if len(get_list_no_images) > 0:
            saved_image = 0
            gateway_list = self.bot.config['ipfs_gateway']['public']
            for each in get_list_no_images:
                if len(gateway_list) <= 2:
                    gateway_list = self.bot.config['ipfs_gateway']['public']
                selected_gw = random.choice(gateway_list)
                url =  selected_gw + each['image'].replace("ipfs://", "")
                if self.bot.config['ipfs_gateway']['use_local_node'] == 1:
                    url = self.bot.config['ipfs_gateway']['local_node'] + each['image'].replace("ipfs://", "")
                try:
                    if each['image'].startswith("ipfs://"):
                        try:
                            # print("Downloading image {}".format(each['image']))
                            async with aiohttp.ClientSession() as session:
                                async with session.get(
                                    url,
                                    timeout=32
                                ) as response:
                                    if response.status == 200:
                                        data = await response.read()
                                        buffer = io.BytesIO(data)
                                        mime = magic.Magic(mime=True)
                                        mime_type = mime.from_buffer(buffer.read(1024))
                                        # Example: > image/png
                                        extension = mime_type.split("/")[-1]
                                        hash_object = hashlib.sha256(data)
                                        hex_dig = str(hash_object.hexdigest())
                                        file_saved = False
                                        if extension == "png":
                                            img = Image.open(buffer).convert("RGBA")
                                            img.save(
                                                self.bot.config['ipfs_gateway']['local_path'] + hex_dig + "." + extension
                                            )
                                            file_saved = True
                                        elif extension == "jpeg":
                                            img = Image.open(buffer).convert("RGB")
                                            img.save(
                                                self.bot.config['ipfs_gateway']['local_path'] + hex_dig + "." + extension
                                            )
                                            file_saved = True
                                        elif extension == "svg+xml":
                                            extension = "svg"
                                            with open(
                                                self.bot.config['ipfs_gateway']['local_path'] + hex_dig + "." + extension, "wb"
                                            ) as file:
                                                file.write(data)
                                                file.close()
                                                file_saved = True
                                        else:
                                            print("Extension {} is not supported yet for {}.".format(extension, each['image']))
                                        if file_saved is True:
                                            await self.utils.update_nft_tracking_meta_image(
                                                mime_type, hex_dig + "." + extension, each['image']
                                            )
                                            saved_image += 1
                                            if saved_image > 0 and saved_image % 5 == 0:
                                                print("Fetched images: {}/{}".format(
                                                    saved_image, len(get_list_no_images)
                                                ))
                                    else:
                                        await asyncio.sleep(1.0)
                        except aiohttp.client_exceptions.InvalidURL:
                            print("Fetch invalid image url: {}".format(url))
                        except asyncio.exceptions.TimeoutError:
                            print("Fetch image timeout for url: {}".format(url))
                            gateway_list.remove(selected_gw)
                        except Exception as e:
                            traceback.print_exc(file=sys.stdout)                                    
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
                    print("Fetched image failed with: {}=> {}".format(
                        each['image'], url
                    ))

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
        if not self.fetch_image_in_nft_cont_meta.is_running():
            self.fetch_image_in_nft_cont_meta.start()
        if not self.fetch_nft_meta_syncing.is_running():
            self.fetch_nft_meta_syncing.start()
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
        if not self.fetch_image_in_nft_cont_meta.is_running():
            self.fetch_image_in_nft_cont_meta.start()
        if not self.fetch_nft_meta_syncing.is_running():
            self.fetch_nft_meta_syncing.start()
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
        self.fetch_image_in_nft_cont_meta.cancel()
        self.fetch_nft_meta_syncing.cancel()
        self.fetch_nft_contract_meta.cancel()
        self.fetch_nft_contract_transfers.cancel()
        self.wallet_eth.cancel()
        self.wallet_nft_eth.cancel()
        self.wallet_polygon.cancel()
        self.wallet_nft_polygon.cancel()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MoralisAPI(bot))