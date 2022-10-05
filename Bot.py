import asyncio
import json
import os
import platform
import random
import sys
import traceback

import discord
from aiohttp import web
from discord.ext import tasks, commands
from discord.ext.commands import AutoShardedBot

from discord.ext.commands import Context

from config import load_config

intents = discord.Intents.default()
intents.members = True
intents.presences = True

bot = AutoShardedBot(command_prefix=commands.when_mentioned, intents=intents, owner_ids=load_config()['discord']['owner_ids'], help_command=None)

bot.config = load_config()
bot.setting = None
bot.server_bot = "DISCORD"
bot.first_message = f"""
Hello, it looks like it's your first time interact with our NFT Bot. 
NFT Bot is an NFT tipping Bot for Discord and only work with slash `/` command.

First of all, you should verify your deposit address(es) with the Bot by commanding `/nftverify`. 
Bot will give you a link to verify and there is no input for password, keys or seed at all.
If you are a desktop user, you will need to signin with Discord (when you already logged with Discord) and a MetaMask.

If you are a mobile user, you need to open the link in MetaMask, Bot will give you a secret key and you paste it into your `/nftverify keyhere`.

NOTE:
* Do not send from any other address besides your verified ones.
* Tipping is instant and off-chain.
* Once you deposited, you can tip to anyone in Discord.
* You need to have sufficient gas to withdraw NFT.

You can read this again with commanding `/nfirst`

Invite link: <{bot.config['discord']['invite_link']}>

Brought to you by Art101.
"""
bot.bot_setting = None
# messages
bot.message_list = []

bot.HELP_MESSAGE = ""
# https://www.smartcontracttoolkit.com/abi
with open('./erc721.json', 'r') as f:
    bot.ERC721_ABI = json.load(f)
with open('./erc1155.json', 'r') as f:
    bot.ERC1155_ABI = json.load(f)
with open('./erc20.json', 'r') as f:
    bot.ERC20_ABI = json.load(f)


@bot.event
async def on_ready() -> None:
    """
    The code in this even is executed when the bot is ready
    """
    print(f"Logged in as {bot.user.name}")
    print(f"discord.py API version: {discord.__version__}")
    print(f"Python version: {platform.python_version()}")
    print(f"Running on: {platform.system()} {platform.release()} ({os.name})")
    print(f"Owner ID: {bot.owner_ids}")
    print(f"Admin: {bot.config['discord']['admin']}")
    print("-------------------")
    try:
        msg = "I am back!"
        channel = bot.get_channel(bot.config['discord']['log_channel'])
        if channel:
            await channel.send(msg)
        else:
            channel = bot.get_channel(bot.config['discord']['log_channel_backup'])
            if channel:
                await channel.send(msg)
    except Exception:
        traceback.print_exc(file=sys.stdout)
    status_task.start()
    await bot.tree.sync()


@tasks.loop(minutes=2.0)
async def status_task() -> None:
    """
    Setup the game status task of the bot
    """
    statuses = ["Start with /", "on Ethereum", "on Polygon", "brought by Art101"]
    await bot.change_presence(activity=discord.Game(random.choice(statuses)))


@bot.event
async def on_message(message: discord.Message) -> None:
    """
    The code in this event is executed every time someone sends a message, with or without the prefix

    :param message: The message that was sent.
    """
    if message.author == bot.user or message.author.bot:
        return
    await bot.process_commands(message)


@bot.event
async def on_command_completion(context: Context) -> None:
    """
    The code in this event is executed every time a normal command has been *successfully* executed
    :param context: The context of the command that has been executed.
    """
    full_command_name = context.command.qualified_name
    split = full_command_name.split(" ")
    executed_command = str(split[0])
    if context.guild is not None:
        print(
            f"Executed {executed_command} command in {context.guild.name} (ID: {context.guild.id}) "
            f"by {context.author} (ID: {context.author.id})"
        )
    else:
        print(f"Executed {executed_command} command by {context.author} (ID: {context.author.id}) in DMs")


@bot.command(usage="reconfig")
@commands.is_owner()
async def reconfig(ctx):
    """Reload configuration"""
    try:
        reload_config()
        await ctx.send(f'{ctx.author.mention}, configuration has been reloaded.')
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


@bot.command(usage="load <cog>")
@commands.is_owner()
async def load(ctx, extension):
    """Load specified cog"""
    try:
        extension = extension.lower()
        await bot.load_extension(f'cogs.{extension}')
        await ctx.send('{}, {} has been loaded.'.format(extension.capitalize(), ctx.author.mention))
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


@bot.command(usage="unload <cog>")
@commands.is_owner()
async def unload(ctx, extension):
    """Unload specified cog"""
    try:
        extension = extension.lower()
        await bot.unload_extension(f'cogs.{extension}')
        await ctx.send('{}, {} has been unloaded.'.format(extension.capitalize(), ctx.author.mention))
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


@bot.command(usage="reload <cog/guilds/utils/all>")
@commands.is_owner()
async def reload(ctx, extension):
    """Reload specified cog"""
    try:
        extension = extension.lower()
        await bot.reload_extension(f'cogs.{extension}')
        await ctx.send('{}, {} has been reloaded.'.format(extension.capitalize(), ctx.author.mention))
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


@bot.event
async def on_command_error(context: Context, error) -> None:
    """
    The code in this event is executed every time a normal valid command catches an error
    :param context: The context of the normal command that failed executing.
    :param error: The error that has been faced.
    """
    if isinstance(error, commands.CommandOnCooldown):
        minutes, seconds = divmod(error.retry_after, 60)
        hours, minutes = divmod(minutes, 60)
        hours = hours % 24
        embed = discord.Embed(
            title="Hey, please slow down!",
            description=f"You can use this command again in {f'{round(hours)} hours' if round(hours) > 0 else ''} "
                        f"{f'{round(minutes)} minutes' if round(minutes) > 0 else ''} "
                        f"{f'{round(seconds)} seconds' if round(seconds) > 0 else ''}.",
            color=0xE02B2B
        )
        await context.send(embed=embed)
    elif isinstance(error, commands.MissingPermissions):
        embed = discord.Embed(
            title="Error!",
            description="You are missing the permission(s) `" + ", ".join(
                error.missing_permissions) + "` to execute this command!",
            color=0xE02B2B
        )
        await context.send(embed=embed)
    elif isinstance(error, commands.MissingRequiredArgument):
        embed = discord.Embed(
            title="Error!",
            # We need to capitalize because the command arguments have no capital letter in the code.
            description=str(error).capitalize(),
            color=0xE02B2B
        )
        await context.send(embed=embed)
    raise error


async def load_cogs() -> None:
    """
    The code in this function is executed whenever the bot will start.
    """
    for file in os.listdir(f"./cogs"):
        if file.endswith(".py"):
            extension = file[:-3]
            try:
                await bot.load_extension(f"cogs.{extension}")
                print(f"Loaded extension '{extension}'")
            except Exception as e:
                exception = f"{type(e).__name__}: {e}"
                print(f"Failed to load extension {extension}\n{exception}")


async def webserver():
    async def handler_get(request):
        return web.Response(text="Hello, world")

    async def handler_post(request):
        try:
            if request.body_exists:
                payload = await request.read()
                full_payload = json.loads(payload)
                user_id = full_payload['user_id']
                address = full_payload['address']
                user_server = full_payload['user_server']
                if str(request.rel_url).startswith("/verify_discord/"):
                    if user_id.isdigit() and user_server == bot.server_bot:
                        found_user = bot.get_user(int(user_id))
                        if found_user:
                            msg = f"You verified your account with address: `{address}`. " \
                                  f"Please only deposit gas or supported NFT from this address!"
                            try:
                                await found_user.send(msg)
                                return web.Response(text="Thank you!")
                            except Exception:
                                traceback.print_exc(file=sys.stdout)
                            channel = bot.get_channel(bot.config['discord']['log_channel'])
                            if channel:
                                await channel.send(f"[DISCORD], User {str(found_user.id)} verified address: `{address}`.")
                            else:
                                channel = bot.get_channel(bot.config['discord']['log_channel_backup'])
                                if channel:
                                    await channel.send(f"[DISCORD], User {str(found_user.id)} verified address: `{address}`.")
                        return web.Response(text="Not found!")
        except Exception:
            traceback.print_exc(file=sys.stdout)
    app = web.Application()
    app.router.add_get('/{tail:.*}', handler_get)
    app.router.add_post('/{tail:.*}', handler_post)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', bot.config['discord']['verify_bind_port'])
    await bot.wait_until_ready()
    await site.start()


def reload_config():
    bot.config = load_config()


async def main():
    async with bot:
        bot.loop.create_task(webserver())
        await bot.start(bot.config['discord']['token'])


asyncio.run(load_cogs())
asyncio.run(main())

