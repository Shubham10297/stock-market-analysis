import asyncio
import json
import sys
import polygon
from polygon.enums import StreamCluster
from tasks import callApi
import requests
from datetime import datetime, timedelta
import os
import subprocess
# from mysql.connector import connection
import MySQLdb

config = json.load(open("config.json", "r"))
ALLOWED_TICKERS = config['tickers']

POLYGON_API_KEY = config['polygon_api_key']

DASHBOARD_PORTS = config['dashboard_ports']

username = config['username']
password = config['password']
database = config['database']
host = config['host']

if sys.platform.startswith('win'):
    # For Windows
    command1 = f"cd /d {os.getcwd()} & "
    command2 = command1 + "celery -A tasks worker --loglevel=info -P gevent"
    command3 = command1 + "python dash_dashboard.py -p "

    subprocess.Popen(['cmd', '/c', 'start', 'cmd', '/k', command2], shell=True)

    for port in DASHBOARD_PORTS:
        command = command3+str(port)
        subprocess.Popen(['cmd', '/c', 'start', 'cmd',
                          '/k', command], shell=True)

elif sys.platform.startswith('darwin'):
    import appscript
    # For macOS
    command1 = f"pushd '{os.getcwd()}'; "
    command2 = command1 + "celery -A tasks worker --loglevel=info"
    command3 = command1+"python dash_dashboard.py -p "

    appscript.app('Terminal').do_script(command2)

    for port in DASHBOARD_PORTS:
        command = command3+str(port)
        appscript.app('Terminal').do_script(command)


DATE_DELTA = 5
current_date = (datetime.now()).strftime(r'%Y-%m-%d')

expire_date = (datetime.now()+timedelta(days=DATE_DELTA)).strftime(r'%Y-%m-%d')

conn = MySQLdb.connect(user=username,
                       host=host,
                       db=database, passwd=password)
cursor = conn.cursor()

cursor.execute(""" CREATE TABLE IF NOT EXISTS trades_info (
id  int PRIMARY KEY AUTO_INCREMENT ,
tick varchar(255),
option_contract varchar(255),
time DATETIME ,
type varchar(255),
val float,
ask float,
bid float,
price float,
expire DATE,
strike_price float)
""")

cursor.execute(
    "ALTER TABLE trades_info ADD INDEX (time);")

cursor.execute(f"delete from trades_info where time < '{current_date}'")
conn.commit()


conn.close()

trade_buffer = []


def get_option_contracts(tick_list):
    contracts = []
    for tick in tick_list:
        data = requests.get(
            rf"https://api.polygon.io/v3/snapshot/options/{tick}?expiration_date.lte={expire_date}&expired=false&apiKey={POLYGON_API_KEY}&limit=250").json()
        contracts = contracts + [x['details']['ticker']
                                 for x in data['results']]
        try:
            next_url = data['next_url']
        except:
            next_url = None
        while next_url:
            try:
                next_url = next_url+f"&apiKey={POLYGON_API_KEY}"
                data = requests.get(next_url).json()
                contracts.extend([x['details']['ticker']
                                  for x in data['results']])
                next_url = data['next_url']

            except:
                next_url = None

    return contracts


# it is possible to create one common message handler for different services.
async def stock_trades_handler(msg):
    print(f'msg received: {msg}')
    global trade_buffer
    trade_buffer.append((msg['t'], msg['q'], msg['sym'], msg['p'], msg['s'],msg['c']))
    if len(trade_buffer) > 100:
        callApi.delay(trade_buffer)
        trade_buffer = []


async def main():

    contracts = get_option_contracts(ALLOWED_TICKERS)
    stream_client = polygon.AsyncStreamClient(
        POLYGON_API_KEY, StreamCluster.OPTIONS)
    await stream_client.subscribe_option_trades(contracts, stock_trades_handler)
    # the lib provides auto reconnect functionality. See docs for info
    await stream_client.handle_messages(reconnect=True)
if __name__ == '__main__':

    asyncio.run(main())
