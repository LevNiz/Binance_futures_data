import asyncio
import aiohttp
import pandas as pd
import time
from datetime import datetime, timezone
import calendar


symbols_time = {
    "BTCUSDT": "2022-10-25",
    "1INCHUSDT": "2022-10-20",
}


def measure_time(func):
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"{execution_time}")
        return result
    return wrapper

async def fetch_klines(session, url):
    async with session.get(url) as response:
        info = await response.json()
        if type(info) is dict:
            return "sleep"
        
        return await response.json()

@measure_time
async def get_klines_iter(symbol, interval, start, end=None, limit=1000):
    df = pd.DataFrame()
    pd.set_option('mode.chained_assignment', None)

    if start is None:
        return

    start = calendar.timegm(datetime.fromisoformat(start).timetuple()) * 1000

    if end is None:
        dt = datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc)
        end = int(utc_time.timestamp()) * 1000
    else:
        end = calendar.timegm(datetime.fromisoformat(end).timetuple()) * 1000

    last_time = None

    async with aiohttp.ClientSession() as session:
        while len(df) == 0 or (last_time is not None and last_time < end):
            url = f'https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit=1500'
            if len(df) == 0:
                url += f'&startTime={start}'
            else:
                url += f'&startTime={last_time}'

            url += f'&endTime={end}'

            klines = await fetch_klines(session, url)
            while klines == 'sleep':
                await asyncio.sleep(60)
                klines = await fetch_klines(session, url)
            
            df2 = pd.DataFrame(klines)
            if len(df2) == 0:
                break
            df2.columns = ['Opentime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Closetime',
                           'Quote asset volume', 'Number of trades', 'Taker by base', 'Taker buy quote', 'Ignore']

            dftmp = df2[['Opentime', 'Open', 'High', 'Low', 'Close', 'Volume']]
            dftmp['Opentime'] = pd.to_datetime(dftmp['Opentime'], unit='ms')
            dftmp['Date'] = dftmp['Opentime'].dt.strftime("%d/%m/%Y")
            dftmp['Time'] = dftmp['Opentime'].dt.strftime("%H:%M:%S")
            dftmp = dftmp.drop(['Opentime'], axis=1)
            column_names = ["Date", "Time", "Open", "High", "Low", "Close", "Volume"]
            dftmp = dftmp.reindex(columns=column_names)
            last_time = int(df2.iloc[-1]['Closetime'])
            df = pd.concat([df, dftmp], axis=0, ignore_index=True)

    filename = f'./csv_data_futures/{symbol}_{interval}_data.csv'
    df.to_csv(filename, sep='\t', index=False)


async def main():
    tasks = [get_klines_iter(symbol, '1m', symbols_time[symbol]) for symbol in symbols_time]
    await asyncio.gather(*tasks)


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

loop.run_until_complete(main())