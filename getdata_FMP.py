import os
import yaml
import httpx
import pandas as pd
import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
from getdata_insightsentry import check_env_varailable, check_if_config_file_exist, check_dataconfig

# Shared HTTPX client with timeout
httpx_client = httpx.AsyncClient(timeout=httpx.Timeout(
    connect=10.0,
    read=30.0,
    write=10.0,
    pool=15.0
))

async def fetch_ohlc_1d(symbol: str, from_: str, to: str):
    api_key = check_env_varailable('FMP_API_KEY')
    url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={symbol}&apikey={api_key}&from={from_}&to={to}"
    for attempt in range(3):
        try:
            response = await httpx_client.get(url)
            response.raise_for_status()
            return response.json()
        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.HTTPError):
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
            else:
                raise


def get_ohlc_1d_sync(symbol: str, from_: str, to: str):
    return asyncio.run(fetch_ohlc_1d(symbol, from_, to))

def make_csv_FMP(data_file_name=None):
   
    load_dotenv()

    data_config_path = check_env_varailable('DATA_CONFIG_PATH')
    check_if_config_file_exist(data_config_path)
    with open(data_config_path, "r") as file:
        data_config = yaml.safe_load(file)
        check_dataconfig(data_config)

    from_data = f"{data_config['Time interval']['Start_year']}-{data_config['Time interval']['Start_month']:02d}-{data_config['Time interval']['Start_day']:02d}"
    to_data = f"{data_config['Time interval']['End_year']}-{data_config['Time interval']['End_month']:02d}-{data_config['Time interval']['End_day']:02d}"
    symbol = data_config['Data info']['Token']

    data_json = get_ohlc_1d_sync(symbol, from_data, to_data)

    if isinstance(data_json, dict) and 'historical' in data_json:
        records = data_json['historical']
    elif isinstance(data_json, list):
        records = data_json
    else:
        raise ValueError("Unexpected response format from FMP API")

    df = pd.DataFrame(records)
    if df.empty:
        raise ValueError("No data returned for the given symbol and dates")

    df.drop(columns=['symbol'], inplace=True)

    df = df.rename(columns={
        "date": "Time",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
        "change": "Change",
        "changePercent": "ChangePercent",
        "vwap": "Vwap"
    })

    df['Time'] = pd.to_datetime(df['Time'])
    df = df.set_index('Time')
    df.sort_index(inplace=True)

    outpudir = "data/"
    os.makedirs(outpudir, exist_ok=True)
    time_interval = (f"[{data_config['Time interval']['Start_year']}-{data_config['Time interval']['Start_month']}-{data_config['Time interval']['Start_day']}]" 
                     +f"[{data_config['Time interval']['End_year']}-{data_config['Time interval']['End_month']}-{data_config['Time interval']['End_day']}]") 
    csv_file_path = f"{outpudir}{data_config['Data info']['Exchange']}:{data_config['Data info']['Token']}_{data_config['Frequency']}_{time_interval}_FMP.CSV"
    df.to_csv(csv_file_path)
    print(f"CSV file '{csv_file_path}' was created!")