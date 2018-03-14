import discord
import asyncio
import aiohttp
import traceback
import datetime
import sqlite3
import sys
import json
import os
import copy
import time
import shlex
import subprocess
from io import BytesIO, StringIO

MAX_RECURSION_DEPTH = 10

class SelfBot:
    def __init__(self):
        self.client = discord.Client()

        self.commands = {}
        self.aliases = {}
        self.scheduler = {}
        self.conf = {"prefix": "//", "token": "Your-token-here"}

        if os.path.isfile('conf.json'):
            with open('conf.json', 'r') as config_file:
                self.conf = json.load(config_file)
        else:
            with open('conf.json', 'w') as config_file:
                config_file.write(json.dumps(self.conf, indent=4))

    def load(self):
        if os.path.isfile('aliases.json'):
            with open('aliases.json', 'r') as aliases_file:
                self.aliases = json.load(aliases_file)
        else:
            with open('aliases.json', 'w') as aliases_file:
                aliases_file.write(json.dumps(self.aliases, indent=4))

        if os.path.isfile('logs.db'):
            self.logdb = sqlite3.connect('logs.db')
            cursor = self.logdb.cursor()
            cursor.execute('pragma foreign_keys=ON')
            cursor.execute('vacuum')
            self.logdb.commit()
        else:
            self.logdb = sqlite3.connect('logs.db')
            cursor = self.logdb.cursor()
            cursor.execute('pragma foreign_keys=ON')
            cursor.execute('''
                CREATE TABLE messages(
                    id INTEGER NOT NULL PRIMARY KEY,
                    guild INTEGER,
                    channel INTEGER NOT NULL,
                    message INTEGER NOT NULL,
                    user INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    time INTEGER NOT NULL,
                    type TINYINT NOT NULL CHECK (type IN (0,1)),
                    FOREIGN KEY(guild) REFERENCES guilds(guild),
                    FOREIGN KEY(channel) REFERENCES channels(channel),
                    FOREIGN KEY(user) REFERENCES users(user))
            ''') # type: 0 (created), 1 (edited)
            cursor.execute('''
                CREATE TABLE guilds(
                    guild INTEGER NOT NULL PRIMARY KEY,
                    name TEXT NOT NULL)
            ''')
            cursor.execute('''
                CREATE TABLE channels(
                    channel INTEGER NOT NULL PRIMARY KEY,
                    guild INTEGER,
                    name TEXT NOT NULL,
                    FOREIGN KEY(guild) REFERENCES guilds(guild))
            ''')
            cursor.execute('''
                CREATE TABLE users(
                    user INTEGER NOT NULL PRIMARY KEY,
                    name TEXT NOT NULL)
            ''')
            cursor.execute('''
                CREATE TABLE nicks(
                    guild INTEGER NOT NULL,
                    user INTEGER NOT NULL,
                    nick TEXT,
                    PRIMARY KEY(guild, user),
                    FOREIGN KEY(guild) REFERENCES guilds(guild),
                    FOREIGN KEY(user) REFERENCES users(user))
            ''')
            cursor.execute('''
                CREATE VIEW log AS
                SELECT id, timestamp, guild, channel, name, text, CASE type WHEN 0 THEN 'CREATE' ELSE 'EDIT' END AS type FROM (
                    SELECT m.id, datetime(m.time, 'unixepoch') as timestamp, g.name as guild, c.name as channel, CASE WHEN n.nick IS NULL THEN u.name ELSE n.nick END as name, m.text, m.type FROM messages AS m
                    INNER JOIN guilds AS g ON m.guild = g.guild
                    INNER JOIN channels AS c ON m.channel = c.channel
                    INNER JOIN users AS u ON m.user = u.user
                    INNER JOIN nicks AS n ON m.guild = n.guild AND m.user = n.user
                    WHERE m.guild IS NOT NULL
                    UNION
                    SELECT m.id, datetime(m.time, 'unixepoch') as timestamp, NULL as guild, c.name as channel, u.name as name, m.text, m.type FROM messages AS m
                    INNER JOIN channels AS c ON m.channel = c.channel
                    INNER JOIN users AS u ON m.user = u.user
                    WHERE m.guild IS NULL
                ) ORDER BY id
            ''')
            self.logdb.commit()

    def cmd(self, description, *aliases, guild=True, pm=True):
        def real_decorator(func):
            name = func.__name__
            self.commands[name] = [func, description, [guild, pm]]
            for alias in aliases:
                if alias not in self.commands:
                    self.commands[alias] = [func, "```\nAlias for {0}{1}.```".format(self.conf['prefix'], name), [guild, pm]]
                else:
                    print("ERROR: Cannot assign alias {0} to command {1} since it is already the name of a command!".format(alias, name))
            return func
        return real_decorator

    async def scheduler_loop(self):
        while not self.client.is_closed():
            for i in list(self.scheduler):
                if self.scheduler[i][0] < datetime.datetime.now():
                    scheduler_array = self.scheduler[i][:]
                    del self.scheduler[i]
                    command_string = scheduler_array[2]
                    print("Executing scheduled command with id {}".format(i))
                    command = command_string.split(' ')[0]
                    parameters = ' '.join(command_string.split(' ')[1:])
                    await self.parse_command(scheduler_array[1], command, parameters, scheduler_array[3])
            await asyncio.sleep(0.1)
        print("Scheduler exiting...")

    def run(self):
        print("Starting...")

        self.load()
                    
        @self.client.event
        async def on_ready():
            self.client.loop.create_task(self.scheduler_loop())

            await self.client.change_presence(status=discord.Status.invisible)
            print('Logged in as')
            print(self.client.user.name+"#"+self.client.user.discriminator)
            print(self.client.user.id)
            print('------')

        @self.client.event
        async def on_message(message):
            cursor = self.logdb.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO users(user, name) VALUES(?,?)
            ''', (
                message.author.id,
                message.author.name
            ))
            if isinstance(message.channel, discord.TextChannel):
                cursor.execute('''
                    INSERT OR REPLACE INTO guilds(guild, name) VALUES(?,?)
                ''', (
                    message.guild.id,
                    message.guild.name
                ))
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO nicks(guild, user, nick) VALUES(?,?,?)
                    ''', (
                        message.guild.id,
                        message.author.id,
                        message.guild.get_member(message.author.id).nick
                    ))
                except:
                    pass
                cursor.execute('''
                    INSERT OR REPLACE INTO channels(channel, guild, name) VALUES(?,?,?)
                ''', (
                    message.channel.id,
                    message.channel.guild.id,
                    message.channel.name
                ))
                cursor.execute('''
                    INSERT INTO messages(guild, channel, message, user, text, time, type) VALUES(?,?,?,?,?,?,?)
                ''', (
                    message.guild.id if message.guild else None,
                    message.channel.id,
                    message.id,
                    message.author.id,
                    message.content + ' ' + ' '.join(a.url for a in message.attachments),
                    int(time.mktime(message.created_at.timetuple())),
                    0
                ))
            else:
                if isinstance(message.channel, discord.DMChannel):
                    cursor.execute('''
                        INSERT OR REPLACE INTO channels(channel, guild, name) VALUES(?,?,?)
                    ''', (
                        message.channel.id,
                        None,
                        message.channel.recipient.name+'#'+message.channel.recipient.discriminator
                    ))
                else: #isinstance(message.channel, discord.GroupChannel)
                    cursor.execute('''
                        INSERT OR REPLACE INTO channels(channel, guild, name) VALUES(?,?,?)
                    ''', (
                        message.channel.id,
                        None,
                        message.channel.name
                    ))
                cursor.execute('''
                    INSERT INTO messages(guild, channel, message, user, text, time, type) VALUES(?,?,?,?,?,?,?)
                ''', (
                    None,
                    message.channel.id,
                    message.id,
                    message.author.id,
                    message.content + ' ' + ' '.join(a.url for a in message.attachments),
                    int(time.mktime(message.created_at.timetuple())),
                    0
                ))
            self.logdb.commit()

            if not message.author.id == self.client.user.id:
                return
            if message.content.startswith(self.conf['prefix']):
                print('Command: ' + message.content)
            else:
                return
            command = message.content[len(self.conf['prefix']):].strip().split(' ')[0].lower()
            parameters = ' '.join(message.content.strip().split(' ')[1:])
            await self.parse_command(message, command, parameters)

        @self.client.event
        async def on_message_edit(before, after):
            cursor = self.logdb.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO users(user, name) VALUES(?,?)
            ''', (
                after.author.id,
                after.author.name
            ))
            if isinstance(after.channel, discord.TextChannel):
                cursor.execute('''
                    INSERT OR REPLACE INTO guilds(guild, name) VALUES(?,?)
                ''', (
                    after.guild.id,
                    after.guild.name
                ))
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO nicks(guild, user, nick) VALUES(?,?,?)
                    ''', (
                        after.guild.id,
                        after.author.id,
                        after.guild.get_member(after.author.id).nick
                    ))
                except:
                    pass
                cursor.execute('''
                    INSERT OR REPLACE INTO channels(channel, guild, name) VALUES(?,?,?)
                ''', (
                    after.channel.id,
                    after.channel.guild.id,
                    after.channel.name
                ))
                cursor.execute('''
                    INSERT INTO messages(guild, channel, message, user, text, time, type) VALUES(?,?,?,?,?,?,?)
                ''', (
                    after.guild.id if after.guild else None,
                    after.channel.id,
                    after.id,
                    after.author.id,
                    after.content + ' ' + ' '.join(a.url for a in after.attachments),
                    int(time.mktime(before.edited_at.timetuple() if before.edited_at else after.created_at.timetuple())),
                    1
                ))
            else:
                if isinstance(after.channel, discord.DMChannel):
                    cursor.execute('''
                        INSERT OR REPLACE INTO channels(channel, guild, name) VALUES(?,?,?)
                    ''', (
                        after.channel.id,
                        None,
                        after.channel.recipient.name+'#'+after.channel.recipient.discriminator
                    ))
                else: #isinstance(after.channel, discord.GroupChannel)
                    cursor.execute('''
                        INSERT OR REPLACE INTO channels(channel, guild, name) VALUES(?,?,?)
                    ''', (
                        after.channel.id,
                        None,
                        after.channel.name
                    ))
                cursor.execute('''
                    INSERT INTO messages(guild, channel, message, user, text, time, type) VALUES(?,?,?,?,?,?,?)
                ''', (
                    None,
                    after.channel.id,
                    after.id,
                    after.author.id,
                    after.content + ' ' + ' '.join(a.url for a in after.attachments),
                    int(time.mktime(before.edited_at.timetuple() if before.edited_at else after.created_at.timetuple())),
                    1
                ))
            self.logdb.commit()

        try:
            while True:
                self.client.run(self.conf['token'], bot=False)
        except RuntimeError:
            pass

    async def parse_command(self, message, command, parameters, recursion=0):
        print("Parsing command {} with parameters {}".format(command, parameters))
        if recursion >= MAX_RECURSION_DEPTH:
            print("Hit max recursion depth of {}".format(MAX_RECURSION_DEPTH))
            await self.reply(message, "ERROR: reached max recursion depth of {}".format(MAX_RECURSION_DEPTH), colour=discord.Colour.red(), footer=message.content.split()[0])
            return
        if isinstance(message.channel, discord.TextChannel):
            pm = False
        else:
            pm = True
        if command in self.commands:
            if pm and not self.commands[command][2][1]:
                await self.reply(message, "ERROR: Command {} may not be used in pm!".format(command), colour=discord.Colour.red(), footer=message.content.split()[0])
                return
            elif not pm and not self.commands[command][2][0]:
                await self.reply(message, "ERROR: Command {} may not be used in a guild!".format(command), colour=discord.Colour.red(), footer=message.content.split()[0])
                return
            else:
                try:
                    await self.commands[command][0](self, message, parameters, recursion=recursion)
                except:
                    traceback.print_exc()
                    try:
                        await self.reply(message, "**Error in command:** {0}\n```py\n{1}```".format(message.content, traceback.format_exc()), colour=discord.Colour.red(), footer=message.content.split()[0])
                    except:
                        print("Printing error message failed, wtf?")
        elif command in self.aliases:
            aliased_command = self.aliases[command].split(' ')[0]
            actual_params = ' '.join(self.aliases[command].split(' ')[1:]).format(parameters, *parameters.split(' '))
            await self.parse_command(message, aliased_command, actual_params, recursion=recursion + 1)
        else:
            await self.reply(message, "Invalid command.", colour=discord.Colour.red(), footer=message.content.split()[0])

    async def reply(self, message, text, colour=discord.Colour.default(), footer="", title=""):
        embed = None
        if message.embeds:
            embed = copy.deepcopy(message.embeds[0])
            if title:
                embed.title = title
            embed.description = text
            embed.colour = colour
        else:
            embed = discord.Embed(title=title, description=text, colour=colour)
            embed.set_footer(text=(footer if footer else message.clean_content), icon_url=self.client.user.avatar_url)
        await message.edit(content='', embed=embed)

    async def _async(self, message, parameters, recursion=0):
        if parameters == '':
            return (self.commands['longasync'][1].format(self.conf['prefix']), 1)
        env = {'message' : message,
               'parameters' : parameters,
               'recursion' : recursion,
               'client' : self.client,
               'channel' : message.channel,
               'author' : message.author,
               'guild' : message.guild,
               'bot' : self}
        env.update(globals())
        old_stdout = sys.stdout
        redirected_output = sys.stdout = StringIO()
        result = None
        exec_string = "async def _temp_exec():\n"
        exec_string += '\n'.join(' ' * 4 + line for line in parameters.split('\n'))
        try:
            exec(exec_string, env)
            result = (redirected_output.getvalue(), 0)
        except Exception:
            traceback.print_exc()
            result = (str(traceback.format_exc()), 2)
        _temp_exec = env['_temp_exec']
        try:
            returnval = await _temp_exec()
            value = redirected_output.getvalue()
            if returnval == None:
                result = (value, 0)
            else:
                result = (value + '\n' + str(returnval), 0)
        except Exception:
            traceback.print_exc()
            result = (str(traceback.format_exc()), 2)
        finally:
            sys.stdout = old_stdout
        return result

    async def _eval(self, message, parameters, recursion=0):
        output = None
        if parameters == '':
            return (self.commands['longeval'][1].format(bot.conf['prefix']), 1)
        try:
            output = eval(parameters)
        except:
            traceback.print_exc()
            return (str(traceback.format_exc()), 2)
        if asyncio.iscoroutine(output):
            output = await output
        return (output, 0)

    async def _exec(self, message, parameters, recursion=0):
        if parameters == '':
            return (self.commands['longexec'][1].format(bot.conf['prefix']), 1)
        old_stdout = sys.stdout
        redirected_output = sys.stdout = StringIO()
        result = None
        try:
            exec(parameters)
            result = (redirected_output.getvalue(), 0)
        except Exception:
            traceback.print_exc()
            result = (str(traceback.format_exc()), 2)
        finally:
            sys.stdout = old_stdout
        return result

    async def _query(self, message, parameters, recursion=0):
        if parameters == '':
            return (self.commands['longquery'][1].format(bot.conf['prefix']), 3)
        cursor = bot.logdb.cursor()
        result = None
        try:
            cursor.execute(parameters)
            rows = list(cursor.fetchall())
            if len(rows) > 0:
                output = '\t\t'.join(d[0] for d in cursor.description) + '\n'
                output += '\n'.join(repr(row) for row in rows)
                result = (output, 0)
            else:
                result = (None, 1)
            bot.logdb.commit()
        except bot.logdb.Error:
            result = (str(traceback.format_exc()), 2)
        return result

    async def _shell(self, message, parameters, recursion=0):
        if parameters == '':
            return ("", None)
        with subprocess.Popen(["bash", "-c", parameters], stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as p:
            return (p.stdout.read(), p.wait())
 
class Util:
    def strfdelta(delta):
        output = [[delta.days, 'day'],
                  [delta.seconds // 60 % 60, 'minute'],
                  [delta.seconds % 60, 'second']]
        for i in range(len(output)):
            if output[i][0] != 1:
                output[i][1] += 's'
        reply_msg = ''
        if output[0][0] != 0:
            reply_msg += "{} {} ".format(output[0][0], output[0][1])
        for i in range(1, len(output)):
            reply_msg += "{} {} ".format(output[i][0], output[i][1])
        reply_msg = reply_msg[:-1]
        return reply_msg

    def convdatestring(datestring):
        datestring = datestring.strip(' ,')
        datearray = []
        funcs = {'d' : lambda x: x * 24 * 60 * 60,
                 'h' : lambda x: x * 60 * 60,
                 'm' : lambda x: x * 60,
                 's' : lambda x: x}
        currentnumber = ''
        for char in datestring:
            if char.isdigit():
                currentnumber += char
            else:
                if currentnumber == '':
                    continue
                datearray.append((int(currentnumber), char))
                currentnumber = ''
        seconds = 0
        if currentnumber:
            seconds += int(currentnumber)
        for i in datearray:
            if i[1] in funcs:
                seconds += funcs[i[1]](i[0])
        return datetime.timedelta(seconds=seconds)

if __name__ == "__main__":
    bot = SelfBot()

    @bot.cmd("```\n{0}shutdown takes no arguments\n\nShuts the bot down.```")
    async def shutdown(bot, message, parameters, recursion=0):
        await bot.reply(message, '*Shutting down*.', colour=discord.Colour.gold())
        await asyncio.sleep(0.2)
        await bot.reply(message, '*Shutting down*..', colour=discord.Colour.gold())
        await asyncio.sleep(0.2)
        await bot.reply(message, '*Shutting down*...', colour=discord.Colour.gold())
        await asyncio.sleep(0.2)
        await bot.reply(message, '*Shutting down*....', colour=discord.Colour.gold())
        await asyncio.sleep(0.2)
        await bot.reply(message, '*Shutting down*.....', colour=discord.Colour.gold())
        await asyncio.sleep(0.2)
        await message.delete()
        await bot.client.logout()
        raise RuntimeError("Shutting down...")

    @bot.cmd("```\n{0}ping takes no arguments\n\nTests the bot's connectivity.```")
    async def ping(bot, message, parameters, recursion=0):
        ts = message.created_at
        await message.edit(content='PONG!')
        latency = message.edited_at - ts
        embed = discord.Embed(title="Ping/Pong", description="{}ms".format(latency.microseconds // 1000))
        embed.set_footer(text="{0}ping {1}".format(bot.conf['prefix'], parameters), icon_url=bot.client.user.avatar_url)
        await message.edit(content='', embed=embed)

    @bot.cmd("```\n{0}longasync <async string>\n\nExecutes <async string> as a coroutine.```", "async")
    async def longasync(bot, message, parameters, recursion=0):
        output, errorcode = await bot._async(message, parameters, recursion)
        if len(output) > 1500:
            output = output[:1500] + "..."
        if errorcode == 1:
            await bot.reply(message, output, colour=discord.Colour.purple(), footer=message.content.split()[0])
        elif errorcode == 2:
            await bot.reply(message, "**Async input:**```py\n{}\n```\n**Output (error):**```py\n{}\n```".format(parameters, output), colour=discord.Colour.red(), footer=message.content.split()[0])
        else:
            await bot.reply(message, "**Async input:**```py\n{}\n```\n**Output:**```py\n{}\n```".format(parameters, output), colour=discord.Colour.green(), footer=message.content.split()[0])

    @bot.cmd("```\n{0}shortasync <async string>\n\nExecutes <async string> as a coroutine.```")
    async def shortasync(bot, message, parameters, recursion=0):
        output, errorcode = await bot._async(message, parameters, recursion)
        if len(output) > 1500:
            output = output[:1500] + "..."
        if errorcode == 1:
            await bot.reply(message, output, colour=discord.Colour.purple(), footer=message.content.split()[0])
        elif errorcode == 2:
            await bot.reply(message, "```py\n{}\n```".format(output), colour=discord.Colour.red(), footer=message.content.split()[0])
        else:
            await bot.reply(message, output, colour=discord.Colour.green(), footer=message.content.split()[0])

    @bot.cmd("```\n{0}silentexec <exec string>\n\nSilently executes <async string> as a coroutine.```")
    async def silentasync(bot, message, parameters, recursion=0):
        await message.delete()
        output, errorcode = await bot._async(message, parameters, recursion)
        
    @bot.cmd("```\n{0}longeval <evaluation string>\n\nEvaluates <evaluation string> using Python's eval() function and returns a result.```", "eval")
    async def longeval(bot, message, parameters, recursion=0):
        output, errorcode = await bot._eval(message, parameters, recursion)
        if len(output) > 1500:
            output = output[:1500] + "..."
        if errorcode == 1:
            await bot.reply(message, output, colour=discord.Colour.purple(), footer=message.content.split()[0])
        elif errorcode == 2:
            await bot.reply(message, "**Eval input:**```py\n{}\n```\n**Output (error):**```py\n{}\n```".format(parameters, output), colour=discord.Colour.red(), footer=message.content.split()[0])
        else:
            await bot.reply(message, "**Eval input:**```py\n{}\n```\n**Output:**```py\n{}\n```".format(parameters, output), colour=discord.Colour.green(), footer=message.content.split()[0])

    @bot.cmd("```\n{0}shorteval <evaluation string>\n\nEvaluates <evaluation string> using Python's eval() function and returns only the result.```")
    async def shorteval(bot, message, parameters, recursion=0):
        output, errorcode = await bot._eval(message, parameters, recursion)
        if len(output) > 1500:
            output = output[:1500] + "..."
        if errorcode == 1:
            await bot.reply(message, output, colour=discord.Colour.purple(), footer=message.content.split()[0])
        else:
            await bot.reply(message, "```py\n{}\n```".format(output), colour=(discord.Colour.red() if errorcode == 2 else discord.Colour.green()), footer=message.content.split()[0])

    @bot.cmd("```\n{0}silenteval <evaluation string>\n\nEvaluates <evaluation string> using Python's eval() function. Mainly used for coroutines.```")
    async def silenteval(bot, message, parameters, recursion=0):
        await message.delete()
        output, errorcode = await bot._eval(message, parameters, recursion)

    @bot.cmd("```\n{0}longexec <exec string>\n\nExecutes <exec string> using Python's exec() function.```", "exec")
    async def longexec(bot, message, parameters, recursion=0):
        output, errorcode = await bot._exec(message, parameters, recursion)
        if len(output) > 1500:
            output = output[:1500] + "..."
        if errorcode == 1:
            await bot.reply(message, output, colour=discord.Colour.purple(), footer=message.content.split()[0])
        elif errorcode == 2:
            await bot.reply(message, "**Exec input:**```py\n{}\n```\n**Output (error):**```py\n{}\n```".format(parameters, output), colour=discord.Colour.red(), footer=message.content.split()[0])
        else:
            await bot.reply(message, "**Exec input:**```py\n{}\n```\n**Output:**```py\n{}\n```".format(parameters, output), colour=discord.Colour.green(), footer=message.content.split()[0])

    @bot.cmd("```\n{0}shortexec <exec string>\n\nExecutes <exec string> using Python's exec() function.```")
    async def shortexec(bot, message, parameters, recursion=0):
        output, errorcode = await bot._exec(message, parameters, recursion)
        if len(output) > 1500:
            output = output[:1500] + "..."
        if errorcode == 1:
            await bot.reply(message, output, colour=discord.Colour.purple(), footer=message.content.split()[0])
        elif errorcode == 2:
            await bot.reply(message, "```py\n{}\n```".format(output), colour=discord.Colour.red(), footer=message.content.split()[0])
        else:
            await bot.reply(message, output, colour=discord.Colour.green(), footer=message.content.split()[0])

    @bot.cmd("```\n{0}silentexec <exec string>\n\nSilently executes <exec string> using Python's exec() function.```")
    async def silentexec(bot, message, parameters, recursion=0):
        await message.delete()
        output, errorcode = await bot._exec(message, parameters, recursion)

    @bot.cmd("```\n{0}longquery <string>\n\nQueries the log database.```", "query", "sql")
    async def longquery(bot, message, parameters, recursion=0):
        output, errorcode = await bot._query(message, parameters, recursion)
        if len(output) > 1500:
            output = output[:1500] + "..."
        if errorcode == 1:
            await bot.reply(message, "**SQL input:**```sql\n{}\n```\n**Success**".format(parameters), colour=discord.Colour.green(), footer=message.content.split()[0])
        elif errorcode == 2:
            await bot.reply(message, "**SQL input:**```sql\n{}\n```\n**Output (error):**```sql\n{}\n```".format(parameters, output), colour=discord.Colour.red(), footer=message.content.split()[0])
        elif errorcode == 3:
            await bot.reply(message, output, colour=discord.Colour.purple(), footer=message.content.split()[0])
        else:
            await bot.reply(message, "**SQL input:**```sql\n{}\n```\n**Output:**```sql\n{}\n```".format(parameters, output), colour=discord.Colour.green(), footer=message.content.split()[0])

    @bot.cmd("```\n{0}shortquery <string>\n\nQueries the log database.```")
    async def shortquery(bot, message, parameters, recursion=0):
        output, errorcode = await bot._query(message, parameters, recursion)
        if len(output) > 1500:
            output = output[:1500] + "..."
        if errorcode == 1:
            await bot.reply(message, "**Success** :thumbsup:", colour=discord.Colour.green(), footer=message.content.split()[0])
        if errorcode == 3:
            await bot.reply(message, output, colour=discord.Colour.purple(), footer=message.content.split()[0])
        else:
            await bot.reply(message, "```sql\n{}\n```".format(output), colour=(discord.Colour.red() if errorcode == 2 else discord.Colour.green()), footer=message.content.split()[0])

    @bot.cmd("```\n{0}silentquery <string>\n\nQueries the log database.```")
    async def silentquery(bot, message, parameters, recursion=0):
        await message.delete()
        output, errorcode = await bot._query(message, parameters, recursion)

    @bot.cmd("```\n{0}longshell <string>\n\nRuns a shell command.```", "shell", "bash")
    async def longshell(bot, message, parameters, recursion=0):
        output, errorcode = await bot._shell(message, parameters, recursion)
        if errorcode is None:
            await bot.reply(message, bot.commands['longshell'][1].format(bot.conf['prefix']), colour=discord.Colour.purple(), footer=message.content.split()[0])
            return
        if len(output) > 1500:
            output = output[:1500] + "..."
        await bot.reply(message, "**Shell command:**```bash\n{}\n```\n**Output (Exit code {}):**```\n{}\n```".format(parameters, errorcode, output.decode("utf-8")), colour=(discord.Colour.green() if errorcode == 0 else discord.Colour.red()), footer=message.content.split()[0])

    @bot.cmd("```\n{0}shortshell <string>\n\nRuns a shell command.```")
    async def shortshell(bot, message, parameters, recursion=0):
        output, errorcode = await bot._shell(message, parameters, recursion)
        if errorcode is None:
            await bot.reply(message, bot.commands['shortshell'][1].format(bot.conf['prefix']), colour=discord.Colour.purple(), footer=message.content.split()[0])
            return
        if len(output) > 1500:
            output = output[:1500] + "..."
        await bot.reply(message, "```\n{}\n```".format(output.decode("utf-8")), colour=(discord.Colour.green() if errorcode == 0 else discord.Colour.red()), footer=message.content.split()[0])

    @bot.cmd("```\n{0}silentshell <string>\n\nRuns a shell command.```")
    async def silentshell(bot, message, parameters, recursion=0):
        await message.delete()
        output, errorcode = await bot._shell(message, parameters, recursion)

    @bot.cmd("```\n{0}userinfo [<user>]\n\nDisplays information about [<user>].```", "uinfo")
    async def userinfo(bot, message, parameters, recursion=0):
        msg_guild = message.guild
        user = parameters.strip("<!@>")
        if not user:
            user = message.author.id
        elif not user.isdigit():
            await bot.reply(message, "ERROR: Please enter a valid user.", discord.Colour.red())
            return
        else:
            user = int(user)
        guild = msg_guild
        if not msg_guild:
            temp_member = discord.utils.get(bot.client.get_all_members(), id=user)
            if temp_member:
                guild = temp_member.guild
        if guild:
            member = guild.get_member(user)
            if not member:
                await bot.reply(message, "ERROR: Please enter a valid member mention or id.", discord.Colour.red())
                return
            embed = discord.Embed(title = "User Info", description = member.name+"#"+member.discriminator)
            embed.add_field(name = "Nickname", value = member.nick, inline = True)
            embed.add_field(name = "Display Name", value = member.display_name, inline = True)
            embed.add_field(name = "ID", value = member.id, inline = True)
            embed.add_field(name = "Bot", value = member.bot, inline = True)
            embed.add_field(name = "Status", value = member.status, inline = True)
            embed.add_field(name = "Playing", value = member.activity, inline = True)
        if msg_guild:
            if member.voice:
                embed.add_field(name = "Voice Channel", value = member.voice.channel, inline = True)
            embed.add_field(name = "Roles", value = '\n'.join([r.name.replace("@everyone", "@\u200beveryone") for r in guild.role_hierarchy if r in member.roles]), inline = False)
            embed.add_field(name = "Joined At", value = "{member.joined_at} ({} ago)".format(Util.strfdelta(datetime.datetime.utcnow() - member.joined_at), member=member), inline = True)
        else:
            await bot.reply(message, "ERROR: Could not get info on user", discord.Colour.red())
            return
        embed.add_field(name = "Created At", value = "{member.created_at} ({} ago)".format(Util.strfdelta(datetime.datetime.utcnow() - member.created_at), member=member), inline = True)
        embed.set_image(url = member.avatar_url)
        embed.set_thumbnail(url = member.avatar_url)
        embed.set_footer(text = "{0}userinfo".format(bot.conf['prefix']), icon_url = bot.client.user.avatar_url)
        embed.set_author(name = bot.client.user.name, icon_url = bot.client.user.avatar_url)
        await message.edit(content="", embed=embed)

    @bot.cmd("```\n{0}guildinfo takes no arguments\n\nDisplays information about the guild this command was used in.```", "ginfo", pm=False)
    async def guildinfo(bot, message, parameters, recursion=0):
        guild = message.guild
        text = 0
        voice = 0
        for channel in guild.channels:
            if isinstance(channel, discord.TextChannel):
                text += 1
            elif isinstance(channel, discord.VoiceChannel):
                voice += 1
        embed = discord.Embed(title = "Server Info", description = guild.name)
        embed.add_field(name = "ID", value = str(guild.id), inline = True)
        owner = '<error - could not get information on owner>'
        try:
            owner = '{owner.name}#{owner.discriminator} ({owner.id})'.format(owner=guild.owner)
        except AttributeError:
            pass
        embed.add_field(name = "Owner", value = owner, inline = True)
        embed.add_field(name = "Created At", value = "{guild.created_at} ({} ago)".format(Util.strfdelta(datetime.datetime.utcnow() - guild.created_at), guild=guild))
        embed.add_field(name = "Region", value = guild.region, inline = True)
        embed.add_field(name = "Members", value = guild.member_count, inline = True)
        embed.add_field(name = "Verification Level", value = guild.verification_level, inline = True)
        embed.add_field(name = "Channels", value = "{} channels ({} text, {} voice)".format(text + voice, text, voice), inline = True)
        if len(guild.roles) > 100:
            roles = '{} roles, showing top 100\n{}\n'.format(len(guild.roles), ', '.join([x.name.replace("@everyone", "@\u200beveryone") for x in guild.role_hierarchy][0:1000]))
        else:
            roles = '{} roles\n{}\n'.format(len(guild.role_hierarchy), ', '.join(map(lambda x: x.name.replace("@everyone", "@\u200beveryone"), guild.role_hierarchy)))
        embed.add_field(name = "Roles", value = roles, inline = False)
        embed.add_field(name = "Emoji Count", value = len(guild.emojis), inline = True)
        embed.set_image(url = guild.icon_url)
        embed.set_thumbnail(url = guild.icon_url)
        embed.set_footer(text = "{0}guildinfo".format(bot.conf['prefix']), icon_url = bot.client.user.avatar_url)
        embed.set_author(name = bot.client.user.name, icon_url = bot.client.user.avatar_url)
        await message.edit(content="", embed=embed)

    @bot.cmd("```\n{0}removeallrole <role name>\n\nRemoves <role name> from all members with that role.```", "rar", pm=False)
    async def removeallrole(bot, message, parameters, recursion=0):
        if parameters == "":
            await bot.reply(message, bot.commands['removeallrole'][1].format(bot.conf['prefix']), colour=discord.Colour.purple())
            return
        guild = message.guild
        role = None
        for temp_role in guild.role_hierarchy:
            if temp_role.name == parameters:
                role = temp_role
                break
        if role:
            members_with_role = []
            for member in guild.members:
                if role in member.roles:
                    members_with_role.append(member)
            for member in members_with_role:
                await member.remove_roles(role)
            await bot.reply(message, "Removed role **{}** from **{}** member{}.".format(role.name, len(members_with_role), '' if len(members_with_role) == 1 else 's'), colour=discord.Colour.green())
        else:
            await bot.reply(message, "ERROR: could not find role named {}. Please ensure the role is spelled correctly and your capitalization is correct.".format(parameters), colour=discord.Colour.red())

    @bot.cmd("```\n{0}changegame [<game>]\n\nChanges your Playing... message to [<game>] or unsets it.```")
    async def changegame(bot, message, parameters, recursion=0):
        if message.guild:
            me = message.guild.me
        else:
            me = list(bot.client.guilds)[0].me
        if parameters == '':
            game = None
        else:
            game = discord.Game(name=parameters)
        await bot.client.change_presence(activity=game, status=me.status)
        await bot.reply(message, ":thumbsup:", colour=discord.Colour.green())

    @bot.cmd("```\n{0}changestatus <status>\n\nChanges your status. Status must be one of: online, idle, dnd, invisible.```")
    async def changestatus(bot, message, parameters, recursion=0):
        parameters = parameters.lower()
        statusmap = {'online' : discord.Status.online,
                     'idle' : discord.Status.idle,
                     'dnd' : discord.Status.dnd,
                     'invisible' : discord.Status.invisible}
        if message.guild:
            me = message.guild.me
        else:
            me = list(bot.client.guilds)[0].me
        if parameters == '':
            msg = "Your current status is " + str(me.status)
        else:
            if parameters in statusmap:
                await bot.client.change_presence(status=statusmap[parameters], activity=me.activity)
                msg = ":thumbsup:"
            else:
                msg = "Status must be one of: online, idle, dnd, invisible."
        await bot.reply(message, msg, colour=discord.Colour.blue())
                
    @bot.cmd("```\n{0}role <add | remove> <mention1 [mention2 ...]> <role name>\n\nAdds or removes <role name> from each member in <mentions>.```", pm=False)
    async def role(bot, message, parameters, recursion=0):
        guild = message.guild
        params = parameters.split(' ')
        if len(params) < 3:
            await bot.reply(message, bot.commands['role'][1].format(bot.conf['prefix']), colour=discord.Colour.purple())
            return
        action = params[0].lower()
        if action in ['add', '+']:
            action = 'add'
        elif action in ['remove', '-']:
            action = 'remove'
        else:
            await bot.reply(message, "ERROR: first parameter must be one of: add, remove.", colour=discord.Colour.red())
            return
        params = params[1:]
        ids = [x.strip('<@!>') for x in params if x.strip('<@!>').isdigit()]
        params = [x for x in params if x.strip('<@!>') not in ids]
        members = [guild.get_member(int(x)) for x in ids]
        members = [x for x in members if x]
        if not members:
            await bot.reply(message, "ERROR: no valid mentions found.", colour=discord.Colour.red())
            return
        role = ' '.join(params)
        if not role:
            await bot.reply(message, "ERROR: no role name given!", colour=discord.Colour.red())
            return
        roles = [x for x in guild.role_hierarchy if x.name == role]
        if not roles:
            await bot.reply(message, "ERROR: could not find role named {}. Please ensure the role is spelled correctly and your capitalization is correct.".format(role), colour=discord.Colour.red())
            return
        role = roles[0]
        if action == 'add':
            function = discord.Member.add_roles
        elif action == 'remove':
            function = discord.Member.remove_roles
        for member in members:
            await function(member, role)
        if action == 'add':
            msg = "Successfully added **{}** to **{}** member{}."
        elif action == 'remove':
            msg = "Successfully removed **{}** from **{}** member{}."
        await bot.reply(message, msg.format(role.name, len(members), '' if len(members) == 1 else 's'), colour=discord.Colour.green())

    @bot.cmd("```\n{0}help <command>\n\nDisplays hopefully helpful information on <command>. Try {0}list for a listing of commands.```")
    async def help(bot, message, parameters, recursion=0):
        if parameters == "":
            parameters = "help"
        if parameters in bot.commands:
            await bot.reply(message, bot.commands[parameters][1].format(bot.conf['prefix']), colour=discord.Colour.purple())
        else:
            await bot.reply(message, "Command {} does not exist.".format(parameters), discord.Colour.red())

    @bot.cmd("```\n{0}listcmds takes no arguments\n\nDisplays a listing of commands.```", "list")
    async def listcmds(bot, message, parameters, recursion=0):
        await bot.reply(message, "Available commands: *{}*".format('*, *'.join(sorted(bot.commands))), discord.Colour.purple())

    @bot.cmd("```\n{0}reply <message>\n\nReplies with <message>. Use with aliases for more fun!```")
    async def reply(bot, message, parameters, recursion=0):
        await message.edit(content=parameters)

    @bot.cmd("```\n{0}say <target> <message>\n\nSends <message> to <target>.```")
    async def say(bot, message, parameters, recursion=0):
        target = parameters.split(' ')[0].strip("<@!#>")
        msg = ' '.join(parameters.split(' ')[1:])
        tgt = bot.client.get_channel(target)
        if not tgt:
            tgt = discord.utils.get(bot.client.get_all_members(), id=target)
        if tgt:
            if msg:
                await tgt.send_message(msg)
                await bot.reply(message, ":thumbsup:", discord.Colour.green())
            else:
                await bot.reply(message, "ERROR: Cannot send an empty message.", colour=discord.Colour.red())
        else:
            await bot.reply(message, "ERROR: Target with id {} not found.".format(target), colour=discord.Colour.red())

    @bot.cmd("```\n{0}echo <message>\n\nSends <message> to the same channel the command was used in.```")
    async def echo(bot, message, parameters, recursion=0):
        if parameters == "":
            await bot.reply(message, "ERROR: Cannot send an empty message.", colour=discord.Colour.red())
            return
        await message.channel.send(parameters)
        await bot.reply(message, ":thumbsup:", discord.Colour.green())

    @bot.cmd("```\n{0}alias <add | edit | remove | list | show> <alias name> [<command string>]\n\nManipulates aliases.```")
    async def alias(bot, message, parameters, recursion=0):
        params = parameters.split(' ')
        if len(params) == 0:
            await bot.reply(message, bot.commands['alias'][1].format(bot.conf['prefix']), colour=discord.Colour.purple())
            return
        action = params[0]
        if action not in ['add', '+', 'edit', '=', 'remove', 'del', 'delete', '-', 'list', 'show']:
            await bot.reply(message, bot.commands['alias'][1].format(bot.conf['prefix']), colour=discord.Colour.purple())
            return
        if len(params) == 1:
            if action in ['add', '+', 'edit', '=']:
                await bot.reply(message, "```\n{0}alias {1} <alias name> <command string>```".format(prefix, action), colour=discord.Colour.purple())
            elif action in ['show', 'remove', '-', 'del', 'delete']:
                await bot.reply(message, "```\n{0}alias {1} <alias name>```".format(prefix, action), colour=discord.Colour.purple())
            elif action == 'list':
                await bot.reply(message, "Available aliases: {}".format(', '.join(sorted(aliases))), colour=discord.Colour.blue())
            return
        alias = params[1]
        if not alias in bot.aliases and action not in ['add', '+']:
            await bot.reply(message, "ERROR: alias {} does not exist!".format(alias), colour=discord.Colour.red())
            return
        if alias in bot.aliases and action in ['add', '+']:
            await bot.reply(message, "ERROR: alias {} already exists. Use `{}alias edit` instead.".format(alias, prefix), colour=discord.Colour.red())
            return
        if len(params) == 2:
            if action in ['add', '+', 'edit', '=']:
                await bot.reply(message, "```\n{0}alias {1} {2} <command string>```".format(prefix, action, alias), colour=discord.Colour.purple())
            elif action == 'show':
                await bot.reply(message, "**{}** is an alias for: ```\n{}\n```".format(alias, bot.aliases[alias]), colour=discord.Colour.purple())
            elif action in ['remove', 'del', 'delete', '-']:
                del bot.aliases[alias]
                await bot.reply(message, "Successfully deleted alias **{}**.".format(alias), colour=discord.Colour.green())
        else:
            commandstring = ' '.join(params[2:])
            bot.aliases[alias] = commandstring
            await bot.reply(message, "Successfully {} alias **{}**.".format(action + "ed", alias), colour=discord.Colour.green())
        with open('aliases.json', 'w') as aliases_file:
            json.dump(bot.aliases, aliases_file)

    @bot.cmd("```\n{0}scheduler <add | remove | list | show> <id or date string> [<command string>]\n\nSchedules commands. Date string is in the "
                      "format #d#h#m#s, corresponding to days, hours, minutes, and seconds. You may omit up to 3 of the aforementioned categories.```")
    async def scheduler(bot, message, parameters, recursion=0):
        params = parameters.split(' ')
        if len(params) == 0:
            await bot.reply(message, bot.commands['scheduler'][1].format(bot.conf['prefix']), colour=discord.Colour.purple())
            return
        action = params[0]
        if action not in ['add', '+', 'remove', 'del', 'delete', '-', 'list', 'show']:
            await bot.reply(message, bot.commands['scheduler'][1].format(bot.conf['prefix']), colour=discord.Colour.purple())
            return
        if len(params) == 1:
            if action in ['add', '+']:
                await bot.reply(message, "```\n{0}scheduler {1} <date string> <command string>```".format(prefix, action), colour=discord.Colour.purple())
            elif action in ['show', 'remove', '-', 'del', 'delete']:
                await bot.reply(message, "```\n{0}scheduler {1} <id>```".format(prefix, action), colour=discord.Colour.purple())
            elif action == 'list':
                await bot.reply(message, "Currently scheduled commands: ```\n{}\n```".format('\n'.join(sorted(["{} (in {}): {}".format(
                    x, Util.strfdelta(bot.scheduler[x][0] - datetime.datetime.now()), bot.scheduler[x][2]) for x in bot.scheduler]))), colour=discord.Colour.blue())
            return
        iddatestring = params[1]
        if not iddatestring in map(str, bot.scheduler) and action not in ['add', '+']:
            await bot.reply(message, "ERROR: id {} does not exist!".format(iddatestring), colour=discord.Colour.red())
            return 
        if len(params) == 2:
            if action in ['add', '+', 'edit', '=']:
                await bot.reply(message, "```\n{0}alias {1} {2} <command string>```".format(prefix, action, iddatestring), colour=discord.Colour.purple())
            elif action == 'show':
                iddatestring = int(iddatestring)
                await bot.reply(message, "ID **{}** is scheduled to run in **{}**: ```\n{}\n```".format(
                            iddatestring, Util.strfdelta(bot.scheduler[iddatestring][0] - datetime.datetime.now()), bot.scheduler[iddatestring][2]), colour=discord.Colour.blue())
            elif action in ['remove', 'del', 'delete', '-']:
                iddatestring = int(iddatestring)
                del bot.scheduler[iddatestring]
                await bot.reply(message, "Successfully deleted scheduled command with id **{}**.".format(iddatestring), colour=discord.Colour.green())
        else:
            if bot.scheduler:
                schid = max(bot.scheduler) + 1
            else:
                schid = 0
            commandstring = ' '.join(params[2:])
            delta = Util.convdatestring(iddatestring)
            bot.scheduler[schid] = [datetime.datetime.now() + delta, message, commandstring, recursion + 1]
            await bot.reply(message, "Successfully scheduled command with id **{}** to run in **{}**: ```\n{}\n```".format(schid, Util.strfdelta(delta), commandstring), colour=discord.Colour.green())

    @bot.cmd("```\n{0}timer <date string>\n\nDisplays a running timer. Date string is in the format #d#h#m#s, corresponding to days, "
                  "hours, minutes, and seconds. You may omit up to 3 of the aforementioned categories.```")
    async def timer(bot, message, parameters, recursion=0):
        if parameters == '':
            await bot.reply(message, bot.commands['timer'][1].format(bot.conf['prefix']), colour=discord.Colour.purple())
            return
        delta = Util.convdatestring(parameters)
        timerend = delta + datetime.datetime.now()
        while timerend > datetime.datetime.now():
            await bot.reply(message, str(timerend - datetime.datetime.now()), colour=discord.Colour.gold())
            await asyncio.sleep(0.5)
        await bot.reply(message, "Timer of **" + parameters + "** finished successfully!", colour=discord.Colour.green())

    @bot.cmd("```\n{0}purge <number of messages>\n\nPurges messages from the current channel.```")
    async def purge(bot, message, parameters, recursion=0):
        if parameters == '':
            await bot.reply(message, bot.commands['purge'][1].format(bot.conf['prefix']), colour=discord.Colour.purple())
            return
        if not parameters.isdigit():
            await bot.reply(message, "Error: Number of messages to purge must be a positive integer.", colour=discord.Colour.red())
            return

        msglist = message.channel.history(limit=int(parameters) + 1)
        await msglist.next()
        async for msg in msglist:
            try:
                await msg.delete()
            except:
                pass
        await bot.reply(message, "Successfully purged **" + parameters + "** messages! :thumbsup:", discord.Colour.green())
        await asyncio.sleep(2)
        await message.delete()

    @bot.cmd("```\n{0}react <string>\n\nReacts using Regional Indicator Symbol Letters.```")
    async def react(bot, message, parameters, recursion=0):
        await message.delete()
        async with message.channel.typing():
            msg = await message.channel.history(limit=2).next()
            for aio in map(lambda c, m=msg: m.add_reaction(chr(ord(c.lower()) - ord("a") + ord("\N{REGIONAL INDICATOR SYMBOL LETTER A}"))), parameters):
                await aio

    @bot.cmd("```\n{0}selfdestruct <date string> <text>\n\nGenerates a self destructing message.```")
    async def selfdestruct(bot, message, parameters, recursion=0):
        params = parameters.split(' ', 1)
        if len(params) < 2:
            await bot.reply(message, bot.commands['selfdestruct'][1].format(bot.conf['prefix']), colour=discord.Colour.purple())
            return

        try:
            delta = Util.convdatestring(params[0])
            timerend = delta + datetime.datetime.now()
        except:
            await bot.reply(message, bot.commands['selfdestruct'][1].format(bot.conf['prefix']), colour=discord.Colour.purple())
            return

        await message.delete()

        embed = discord.Embed(title="", description=params[1])
        embed.set_footer(text="This message will self-destruct in {}".format(str(timerend - datetime.datetime.now())))
        msg = await message.channel.send(embed=embed)
        await asyncio.sleep(0.5)

        while timerend > datetime.datetime.now():
            embed.set_footer(text="This message will self-destruct in {}".format(str(timerend - datetime.datetime.now())))
            await msg.edit(embed=embed)
            await asyncio.sleep(0.5)
        await msg.delete()

    @bot.cmd("```\n{0}move <source> <target>\n\nMoves users from one voice channel to another.```")
    async def move(bot, message, parameters, recursion=0):
        params = shlex.split(parameters)
        if len(params) != 2:
            await bot.reply(message, bot.commands['move'][1].format(bot.conf['prefix']), colour=discord.Colour.purple())
            return

        voicechannels = []
        for channel in message.guild.channels:
            if isinstance(channel, discord.VoiceChannel):
                voicechannels.append(channel)
        source = discord.utils.get(voicechannels, name=params[0])
        if not source:
            await bot.reply(message, "**Error**: No such voice channel {}".format(params[0]), colour=discord.Colour.red())
            return
        target = discord.utils.get(voicechannels, name=params[1])
        if not target:
            await bot.reply(message, "**Error**: No such voice channel {}".format(params[1]), colour=discord.Colour.red())
            return

        users = source.members
        for member in users:
            await member.edit(voice_channel=target)

        await bot.reply(message, "Moved {} users from {} to {} :thumbsup:".format(len(users), source, target), colour=discord.Colour.green())

    @bot.cmd("```\n{0}seen [<user>]\n\nDisplays seen information about [<user>].```")
    async def seen(bot, message, parameters, recursion=0):
        user = parameters.strip("<!@>")
        if not user:
            user = message.author.id
        elif not user.isdigit():
            await bot.reply(message, "ERROR: Please enter a valid user.", colour=discord.Colour.red())
            return
        else:
            user = int(user)

        cursor = bot.logdb.cursor()
        cursor.execute('''
            SELECT datetime(m.time, 'unixepoch') as timestamp, g.name as guild, c.name as channel, CASE WHEN n.nick IS NULL THEN u.name ELSE n.nick END as name, m.text, m.type FROM messages AS m
            INNER JOIN guilds AS g ON m.guild = g.guild
            INNER JOIN channels AS c ON m.channel = c.channel
            INNER JOIN users AS u ON m.user = u.user
            INNER JOIN nicks AS n ON m.guild = n.guild AND m.user = n.user
            WHERE m.guild IS NOT NULL AND m.user = ?
            ORDER BY m.time DESC
            LIMIT 1
        ''', (user,))
        row = cursor.fetchone()
        if not row:
            await bot.reply(message, "I have not seen {} recently.".format(parameters), colour=discord.Colour.orange())
        else:
            if row[5] == 0:
                await bot.reply(message, 'Last saw **{3}** messaging "{4}" in channel **#{2}** on server **{1}** at **{0} UTC**'.format(*row), colour=discord.Colour.green())
            else:
                await bot.reply(message, 'Last saw **{3}** editing a message to "{4}" in channel **#{2}** on server **{1}** at **{0} UTC**'.format(*row), colour=discord.Colour.green())

    bot.run()

