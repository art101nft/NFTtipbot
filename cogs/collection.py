import sys
import traceback
from datetime import datetime
from typing import List

import discord
from discord import app_commands
from discord.ext import commands
import json
from cogs.utils import Utils, MenuPage


class Collection(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.utils = Utils(self.bot)

    @app_commands.command(
        name="nftbrowse",
        description="Show your NFT."
    )
    async def command_browse(
        self,
        interaction: discord.Interaction
    ) -> None:
        """ /nftbrowse"""
        await interaction.response.send_message(f"{interaction.user.mention} browsing your NFTs...")

        get_user_info = await self.utils.get_user_info(str(interaction.user.id), self.bot.server_bot)
        if get_user_info is None:
            await interaction.edit_original_response(content=self.bot.first_message)
            await self.utils.insert_user_info(str(interaction.user.id), self.bot.server_bot, 1)
            return
        await self.utils.insert_user_info(str(interaction.user.id), self.bot.server_bot, 1)

        try:
            list_user_assets = await self.utils.get_list_user_assets(str(interaction.user.id), self.bot.server_bot)
            if len(list_user_assets) == 0:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, you don't have any NFT.")
            else:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, you have {str(len(list_user_assets))} "
                            f"NFTs in your fund wallet.")
                try:
                    all_pages = []
                    for each in list_user_assets:
                        embed = discord.Embed(
                            title=each['name'],
                            description=each['desc'][:1000],
                            timestamp=datetime.now())
                        try:
                            image_url = json.loads(each['meta'])['image']
                            if image_url.startswith("ipfs://"):
                                image_url = image_url.replace("ipfs://", "https://gateway.moralisipfs.com/ipfs/")
                            elif image_url.startswith("https://ipfs.io/ipfs/"):
                                image_url = image_url.replace("https://ipfs.io/ipfs/", "https://gateway.moralisipfs.com/ipfs/")
                            embed.set_image(url=image_url)
                            # print("{} - {}".format(each['name'], image_url))
                        except Exception:
                            traceback.print_exc(file=sys.stdout)  
                        try:
                            if self.bot.config['rarity']['enable'] == 1:
                                get_rarity = await self.utils.get_rarity_by_item_list_id(each['item_id'])
                                if get_rarity:
                                    trait_count_nos = get_rarity['trait_count']
                                    rarity_score = get_rarity['rarity_score']
                                    embed.add_field(
                                        name="Rarity Rank {}".format(get_rarity['rank']),
                                        value=f"Trait Count: {str(trait_count_nos)}\nTotal rarity score: "
                                              f"{str(rarity_score)}",
                                        inline=False
                                    )
                        except Exception:
                            traceback.print_exc(file=sys.stdout)  
                        
                        embed.set_footer(text=f'Owner {interaction.user.name}#{interaction.user.discriminator}',
                                         icon_url=interaction.user.display_avatar)
                        all_pages.append(embed)
                    view = MenuPage(interaction, all_pages, timeout=10, disable_remove=True)
                    view.message = await interaction.edit_original_response(content=None, embed=all_pages[0], view=view)
                except Exception:
                    traceback.print_exc(file=sys.stdout)                    
        except Exception:
            traceback.print_exc(file=sys.stdout)

    @app_commands.command(
        name="rarity",
        description="Built-in rarity."
    )
    async def command_rarity(
        self,
        interaction: discord.Interaction,
        nft_item_list_id: str
    ) -> None:
        """ /rarity <nft_item_list_id>"""
        await interaction.response.send_message(f"{interaction.user.mention} loading rarity...")
        try:
            get_nft = None
            try:
                nft_item_list_id = int(nft_item_list_id)
                get_nft = await self.utils.get_rarity_by_item_list_id(nft_item_list_id)
            except ValueError:
                get_nft = await self.utils.get_rarity_by_item_list_name(nft_item_list_id)
            
            if get_nft is None:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, such NFT was not found in our rarity yet. Try again."
                )
                return
            else:
                trait_count_nos = get_nft['trait_count']
                rarity_score = get_nft['rarity_score']
                embed = discord.Embed(
                    title="{} RANK #{}".format(get_nft['name'][:32], get_nft['rank']),
                    description=f"```Trait Count: {str(trait_count_nos)}\nTotal rarity score: {str(rarity_score)}```",
                    timestamp=datetime.now()
                )
                attributes = json.loads(get_nft['attributes_dump'])
                i = 0
                max_embed = 20
                missing_score = 0.0
                for each_attr in attributes:
                    if i < max_embed:
                        embed.add_field(
                            name="{} | {}".format(each_attr['trait_type'], each_attr['value']),
                            value="{}".format(each_attr['rarity_score']),
                            inline=True
                        )
                        i += 1
                    else:
                        missing_score += each_attr['rarity_score']
                if i < max_embed:
                    missing_attributes = json.loads(get_nft['missing_traits_dump'])
                    for each_attr in missing_attributes:
                        if i < max_embed:
                            embed.add_field(
                                name="{} | {}".format(each_attr['trait_type'], "None"),
                                value="{}".format(each_attr['rarity_score']),
                                inline=True
                            )
                            i += 1
                        else:
                            missing_score += each_attr['rarity_score']
                if missing_score > 0:
                    embed.add_field(name="Other traits", value="{}".format(missing_score), inline=False)
                try:
                    image_url = json.loads(get_nft['meta'])['image']
                    if image_url.startswith("ipfs://"):
                        image_url = image_url.replace("ipfs://", "https://gateway.moralisipfs.com/ipfs/")
                    elif image_url.startswith("https://ipfs.io/ipfs/"):
                        image_url = image_url.replace("https://ipfs.io/ipfs/", "https://gateway.moralisipfs.com/ipfs/")
                    embed.set_thumbnail(url=image_url)
                except Exception:
                    traceback.print_exc(file=sys.stdout)
                embed.add_field(name="Collection Name", value=get_nft['collection_name'], inline=False)
                embed.add_field(name="Contract", value=get_nft['contract'], inline=False)
                embed.set_footer(
                    text=f"by pluton | Requested by: {interaction.user.name}#{interaction.user.discriminator}"
                )
                await interaction.edit_original_response(
                    content=None, embed=embed
                )
        except Exception:
            traceback.print_exc(file=sys.stdout)

    @command_rarity.autocomplete('nft_item_list_id')
    async def rarity_item_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str
    ) -> List[app_commands.Choice[str]]:
        # Do stuff with the "current" parameter, e.g. querying it search results...
        list_auto_nfts = await self.utils.get_rarity_by_item_name_search(current, 20)
        nft_list_names = []
        nft_item_list_ids = []
        if len(list_auto_nfts) > 0:
            for each in list_auto_nfts:
                if each['nft_item_list_id'] not in nft_item_list_ids:
                    nft_list_names.append(each['name'])
                    nft_item_list_ids.append(str(each['nft_item_list_id']))
            return [
                app_commands.Choice(name=item, value=nft_item_list_ids[nft_list_names.index(item)])
                for item in nft_list_names if current.lower() in item.lower()
            ]
        else:
            nft_list_names = ["N/A"]
            return [
                app_commands.Choice(name=item, value=item)
                for item in nft_list_names if current.lower() in item.lower()
            ]

    @app_commands.command(
        name="nftcollection",
        description="Show NFT collection supported by Bot."
    )
    async def command_collection(
        self,
        interaction: discord.Interaction,
        collection: str
    ) -> None:
        """ /nftcollection <collection> """
        await interaction.response.send_message(f"{interaction.user.mention} loading collection...")

        get_user_info = await self.utils.get_user_info(str(interaction.user.id), self.bot.server_bot)
        if get_user_info is None:
            await interaction.edit_original_response(content=self.bot.first_message)
            await self.utils.insert_user_info(str(interaction.user.id), self.bot.server_bot, 1)
            return
        await self.utils.insert_user_info(str(interaction.user.id), self.bot.server_bot, 1)

        try:
            get_collection = await self.utils.get_contract_by_id(int(collection))
            if get_collection is None:
                await interaction.edit_original_response(
                    content=f"{interaction.user.mention}, could not find such collection. Try again later.")
                return
            else:
                embed = discord.Embed(
                    title=get_collection['collection_name'],
                    description=get_collection['desc'][:1000],
                    timestamp=datetime.now())
                embed.set_author(name=get_collection['collection_name'],
                                 icon_url=get_collection['collection_thumb_url'])
                embed.set_thumbnail(url=get_collection['collection_thumb_url'])
                if get_collection['contract']:
                    embed.add_field(name="Contract",
                                    value="[{}]({})".format(
                                        get_collection['contract'], get_collection['explorer_link']
                                    ),
                                    inline=False)
                if get_collection['url']:
                    embed.add_field(name="Website",
                                    value=get_collection['url'],
                                    inline=False)
                if get_collection['market_link']:
                    embed.add_field(name="Marketplace",
                                    value=get_collection['market_link'],
                                    inline=False)
                embed.set_footer(text=f'Requested by {interaction.user.name}#{interaction.user.discriminator}',
                                 icon_url=interaction.user.display_avatar)
                await interaction.edit_original_response(content=None, embed=embed)
        except Exception:
            traceback.print_exc(file=sys.stdout)

    @command_collection.autocomplete('collection')
    async def collection_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str
    ) -> List[app_commands.Choice[str]]:
        # Do stuff with the "current" parameter, e.g. querying it search results...
        list_collections = await self.utils.get_list_contract_collection_search(current, 20)
        collection_list = []
        collection_ids = []
        if len(list_collections) > 0:
            for each in list_collections:
                if each['contract_id'] not in collection_ids:
                    collection_list.append(each['collection_name'])
                    collection_ids.append(str(each['contract_id']))
            return [
                app_commands.Choice(name=item, value=collection_ids[collection_list.index(item)])
                for item in collection_list if current.lower() in item.lower()
            ]
        else:
            collection_list = ["N/A"]
            return [
                app_commands.Choice(name=item, value=item)
                for item in collection_list if current.lower() in item.lower()
            ]

    async def cog_load(self) -> None:
        pass

    async def cog_unload(self) -> None:
        pass


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Collection(bot))
