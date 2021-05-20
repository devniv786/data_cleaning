import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import re


def process_futures_data(df_futures: pd.DataFrame) -> pd.DataFrame:
    try:
        df_futures["Symbol"] = df_futures["Ticker"].str.split("-")
        df_futures["Symbol_Name"] = df_futures["Symbol"].apply(lambda x: x[0])
        df_futures["Future_Series"] = df_futures["Symbol"].apply(lambda x: x[1])
        df_futures.drop(columns="Symbol",inplace=True)
        df_futures[["Expiry","Strike_Price"]] = np.NaN
        return df_futures
    except Exception:
        error_info = sys.exc_info()
        print("Error occured while preparing futures data: {}".format(error_info[1]))


def calculate_options_parameters(ticker:str):
    idx_num = re.search(r"\d", ticker).start()
    symbol = ticker[:idx_num]
    expiry = ticker[idx_num:idx_num + 7]
    strike_idx = re.search(r"\D", ticker[idx_num + 7:]).start()
    strike_price = ticker[idx_num + 7:][:strike_idx]
    option_type = ticker[idx_num + 7:][strike_idx:strike_idx + 2]
    return [symbol,expiry,strike_price,option_type]

def process_options_data(df_options: pd.DataFrame) -> pd.DataFrame:
    try:

        df_options["Symbol"], df_options["Expiry"],df_options["Strike_Price"],df_options["Option_Type"] = zip(*df_options["Ticker"].apply(lambda x : calculate_options_parameters(x)))
    #   df_options["Expiry"] = df_options["Ticker"].apply(lambda x : x[idx_num:idx_num + 7])
    #   df_options["Strike_Price"] = df_options["Ticker"].apply(lambda x : x[idx_num + 7:][:strike_idx])
    #   df_options["Option_Type"] = df_options["Ticker"].apply(lambda x : x[idx_num + 7:][strike_idx:strike_idx + 2])
        return df_options
    except Exception:
        error_info = sys.exc_info()
        print("Error occured while preparing futures data: {}".format(error_info[1]))


if __name__=="__main__":
    # print(os.getcwd())
    df = pd.read_csv(os.path.join(os.path.dirname((os.path.realpath(__file__))),Path("Data\\NFO.csv")))
    df.drop(columns=['Unnamed: 9', 'OUTPUT>>>', 'INDEX', 'Ticker.1', 'Date.1', 'Time.1', 'Open.1', 'High.1', 'Low.1',
                     'Close.1', 'Volume.1', 'Open Interest.1', 'SYMBOL', 'EXPIRY', 'STRIKE', 'TYPE'], inplace=True)
    pattern = re.compile(r"(-I|-II|-III)", re.I)
    df_futures_raw = df[df["Ticker"].str.contains(pattern)]
    df_futures_processed = process_futures_data(df_futures_raw)
    df_options_raw = df[df["Ticker"].str.contains("PE.NFO|CE.NFO")]
    df_options_processed = process_options_data(df_options_raw)
    print("Process completed")
    df_futures_processed.to_csv("Futures.csv",index=False)
    df_options_processed.to_csv("Options.csv",index=False)