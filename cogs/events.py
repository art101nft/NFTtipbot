import discord
from discord.ext import commands, tasks
import traceback, sys
from cogs.utils import Utils
import time
import asyncio


# Cog class
class Events(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot: commands.Bot = bot
        self.utils = Utils(self.bot)
        self.max_saving_message = 100
        self.is_saving_message = False
        self.message_id_list = []

    @tasks.loop(seconds=20.0)
    async def process_saving_message(self):
        time_lap = 10  # seconds
        # Check if task recently run @bot_task_logs
        await asyncio.sleep(time_lap)
        if len(self.bot.message_list) > 0:
            # saving_message
            if self.is_saving_message is True:
                return
            else:
                self.is_saving_message = True
            try:
                saving = await self.utils.insert_discord_message(list(set(self.bot.message_list)))
                if saving > 0:
                    self.bot.message_list = []
            except Exception:
                traceback.print_exc(file=sys.stdout)
            self.is_saving_message = False

    @commands.Cog.listener()
    async def on_message(self, message):
        # should ignore webhook message
        if message is None:
            return

        if hasattr(message, "channel") and hasattr(message.channel, "id") and message.webhook_id:
            return

        if hasattr(message, "channel") and \
            hasattr(message.channel, "id") and message.author.bot == False and message.author != self.bot.user:
            if message.id not in self.message_id_list:
                try:
                    self.bot.message_list.append((str(message.guild.id), message.guild.name, str(message.channel.id),
                                                  message.channel.name, str(message.author.id),
                                                  "{}#{}".format(message.author.name, message.author.discriminator),
                                                  str(message.id), int(time.time())))
                    self.message_id_list.append(message.id)
                except Exception:
                    pass
            if len(self.bot.message_list) >= self.max_saving_message:
                # saving_message
                if self.is_saving_message is True:
                    return
                else:
                    self.is_saving_message = True
                try:
                    saving = await self.utils.insert_discord_message(list(set(self.bot.message_list)))
                    if saving > 0:
                        self.bot.message_list = []
                except Exception:
                    traceback.print_exc(file=sys.stdout)
                self.is_saving_message = False

    @commands.Cog.listener()
    async def on_message_delete(self, message):
        # should ignore webhook message
        if message is None:
            return

        if hasattr(message, "channel") and hasattr(message.channel, "id") and message.webhook_id:
            return

        if hasattr(message, "channel") and \
            hasattr(message.channel, "id") and message.author.bot == False and message.author != self.bot.user:
            if message.id in self.message_id_list:
                self.is_saving_message = True
                # saving_message
                try:
                    saving = await self.utils.insert_discord_message(list(set(self.bot.message_list)))
                    if saving > 0:
                        self.bot.message_list = []
                except Exception:
                    traceback.print_exc(file=sys.stdout)
                self.is_saving_message = False
            # Try delete from database
            self.is_saving_message = True
            try:
                await self.utils.delete_discord_message(str(message.id), str(message.author.id))
            except Exception:
                traceback.print_exc(file=sys.stdout)
            self.is_saving_message = False

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        await self.utils.log_to_channel(
            self.bot.config['discord']['log_channel'],
            f"Bot joined a new guild `{guild.id} / {guild.name}`"
        )

    @commands.Cog.listener()
    async def on_guild_remove(self, guild):
        await self.utils.log_to_channel(
            self.bot.config['discord']['log_channel'],
            f"Bot removed from guild `{guild.id} / {guild.name}`."
        )

    @commands.Cog.listener()
    async def on_button_click(
            self, interaction: discord.MessageInteraction, button: discord.ui.Button
    ):
        if interaction.message.author == self.bot.user and button.custom_id == "close_any_message":
            try:
                await interaction.message.delete()
            except Exception:
                traceback.print_exc(file=sys.stdout)
                try:
                    _msg: discord.Message = await interaction.channel.fetch_message(interaction.message.id)
                    await _msg.delete()
                except discord.errors.NotFound:
                    return
                except Exception:
                    traceback.print_exc(file=sys.stdout)

    @commands.Cog.listener()
    async def on_ready(self):
        if not self.process_saving_message.is_running():
            self.process_saving_message.start()

    async def cog_load(self) -> None:
        if not self.process_saving_message.is_running():
            self.process_saving_message.start()

    async def cog_unload(self) -> None:
        self.process_saving_message.cancel()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Events(bot))