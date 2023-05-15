import sys
from celery import Celery
from datetime import datetime, timedelta
import re
import asyncio
import aiohttp
import json
# from mysql.connector import connection
import MySQLdb

config = json.load(open("config.json", "r"))
POLYGON_API_KEY = config['polygon_api_key']
INDICES = config['tickers_which_are_indices']
BROKER_URL = config['broker_url']

username = config['username']
password = config['password']
database = config['database']
host = config['host']

IGNORED_CONDITIONS = {232,233,234,235,236,238,239,256}

app = Celery(
    name='tasks',
    broker=BROKER_URL,
    backend='db+sqlite:///db.sqlite3')


async def get_data_async(s, trade):
    try:
        time = datetime.now().strftime(r'%Y-%m-%d %H:%M:%S')  # trade[0]
        option_contract = trade[2]
        price = trade[3]
        size = trade[4]
        condition = trade[5]

        parse = re.findall("[a-zA-Z]+", option_contract)
        tick = parse[1].strip()
        type = parse[2].strip()

        if tick == 'SPXW':
            tick = 'SPX'
        if tick in INDICES:
            url = rf"https://api.polygon.io/v3/snapshot/options/I:{tick}/{option_contract}?apiKey={POLYGON_API_KEY}"
        else:
            url = rf"https://api.polygon.io/v3/snapshot/options/{tick}/{option_contract}?apiKey={POLYGON_API_KEY}"

        if set(condition).issubset(IGNORED_CONDITIONS):
            print("Does not meet the conditions")
            return
        
        async with s.get(url) as r:
            data = await r.json()

            if tick == 'SPX':
                val = data['results']['greeks']['delta']*size*1000
            else:
                val = data['results']['greeks']['delta']*size*100

            ask = data['results']['last_quote']['ask']
            bid = data['results']['last_quote']['bid']
            expire = data['results']['details']['expiration_date']
            strike_price = data['results']['details']['strike_price']

            return (tick, option_contract, time, type, val, ask, bid, price, expire, strike_price)
    except Exception as e:
        print(e)
        return


async def fetch_all(s, trades):
    tasks = []
    for trade in trades:
        task = asyncio.create_task(get_data_async(s, trade))
        tasks.append(task)
    res = await asyncio.gather(*tasks)
    return res


async def main(trades):
    async with aiohttp.ClientSession() as session:
        try:
            result = await fetch_all(session, trades)

            result = [x for x in result if x is not None]
            print("the length of result is ", len(result))
            conn = MySQLdb.connect(user=username,
                                   host=host,
                                   db=database, passwd=password)
            cursor = conn.cursor()
            cursor.executemany(
                "INSERT INTO trades_info (tick, option_contract, time, type, val, ask, bid, price, expire,strike_price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", result)
            conn.commit()
            conn.close()
        except Exception as e:
            print(e)
            conn.rollback()
            conn.close()


@app.task
def callApi(trades):
    # print("input trades--->", trades)
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # 'RuntimeError: There is no current event loop...'
        loop = None

    if loop and loop.is_running():
        print('Async event loop already running. Adding coroutine to the event loop.')
        tsk = loop.create_task(main(trades))
    else:
        print('Starting new event loop.')
        asyncio.run(main(trades))


# celery -A tasks worker --loglevel=info -P gevent
