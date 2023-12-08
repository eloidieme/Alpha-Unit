import pytz
import requests
import threading
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import yfinance as yf

from utils import load_pickle, save_pickle, Alpha


def get_sp500_tickers() -> list:
    res = requests.get(
        "https://en.wikipedia.org/wiki/List_of_S&P_500_companies")
    soup = BeautifulSoup(res.content, 'html')
    table = soup.find_all('table')[0]
    df = pd.read_html(str(table))
    tickers = list(df[0].Symbol)
    return tickers


def get_history(ticker: str, start_date: datetime, end_date: datetime, granularity="1d", tries=0) -> pd.DataFrame:
    try:
        df = yf.Ticker(ticker).history(start=start_date, end=end_date,
                                       interval=granularity, auto_adjust=True).reset_index()
    except Exception as err:
        if tries < 5:
            get_history(ticker, start_date, end_date, granularity, tries + 1)
        return pd.DataFrame()

    df = df.rename(columns={
        "Date": "datetime",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })
    if df.empty:
        return pd.DataFrame()

    df["datetime"] = df["datetime"].dt.tz_convert(pytz.utc)
    df = df.drop(columns=["Dividends", "Stock Splits"])
    df = df.set_index("datetime", drop=True)
    return df


def get_histories(tickers: list, period_start: list, period_end: list, granularity="1d"):
    dfs = [None]*len(tickers)

    def _helper(i):
        df = get_history(tickers[i], period_start[i],
                         period_end[i], granularity=granularity)
        dfs[i] = df
    threads = [threading.Thread(target=_helper, args=(i,))
               for i in range(len(tickers))]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]
    tickers = [tickers[i] for i in range(len(tickers)) if not dfs[i].empty]
    dfs = [df for df in dfs if not df.empty]
    return tickers, dfs


def get_ticker_dfs(start: datetime, end: datetime):
    try:
        tickers, ticker_dfs = load_pickle(f"data/dataset_{start}_{end}.obj")
    except Exception as err:
        tickers = get_sp500_tickers()
        starts = [start]*len(tickers)
        ends = [end]*len(tickers)
        tickers, dfs = get_histories(
            tickers, starts, ends)
        ticker_dfs = {ticker: df for ticker, df in zip(tickers, dfs)}
        save_pickle(f"data/dataset_{start}_{end}.obj", (tickers, ticker_dfs))
    return tickers, ticker_dfs


start_d = datetime(2010, 1, 1, 5, tzinfo=pytz.utc)
end_d = datetime(2023, 11, 29, 5, tzinfo=pytz.utc)
tickers, ticker_dfs = get_ticker_dfs(start=start_d, end=end_d)
testfor = 20
tickers = tickers[:testfor]
alpha = Alpha(insts=tickers, dfs=ticker_dfs, start=start_d, end=end_d)
portfolio_df = alpha.run_simulation()
