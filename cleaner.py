import psycopg2
from sqlalchemy import create_engine
import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path
import warnings

warnings.filterwarnings("ignore")
pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)
import re


def week_number_of_month(date_value):
    week_number = (
        date_value.isocalendar()[1] - date_value.replace(day=1).isocalendar()[1] + 1
    )
    if week_number == -46:
        week_number = 6
    return week_number


def process_futures_data(df_futures: pd.DataFrame) -> pd.DataFrame:
    try:
        df_futures["Symbol_Temp"] = df_futures["Ticker"].str.split("-")
        df_futures["Symbol"] = df_futures["Symbol_Temp"].apply(lambda x: x[0])
        df_futures["Future_Series"] = df_futures["Symbol_Temp"].apply(lambda x: x[1])
        df_futures["Future_Series"] = df_futures["Future_Series"].replace(
            {"I.NFO": "FUT-1", "II.NFO": "FUT-2", "III.NFO": "FUT-3"}
        )
        df_futures["Expiry_Type"] = "Monthly"
        df_futures["DateTime"] = df_futures["Date"] + " " + df_futures["Time"]
        df_futures.drop(columns="Symbol_Temp", inplace=True)
        df_futures[["Expiry", "Strike_Price", "Option_Type"]] = np.NaN
        return df_futures
    except Exception:
        error_info = sys.exc_info()
        print("Error occured while preparing futures data: {}".format(error_info[1]))


def calculate_options_parameters(ticker: str):
    if "TV18BRDCST" in ticker:
        # special case
        idx_num1 = re.search(r"\d", ticker).end()
        idx_num2 = re.search(r"\d", ticker[idx_num1 + 1 :])
        symbol = ticker[: idx_num1 + idx_num2.start() + 1]
        expiry = ticker[
            idx_num1 + idx_num2.start() + 1 : idx_num1 + idx_num2.start() + 1 + 7
        ]
        strike_idx = re.search(
            r"\D", ticker[idx_num1 + idx_num2.start() + 1 + 7 :]
        ).start()
        strike_price = ticker[idx_num1 + idx_num2.start() + 1 + 7 :][:strike_idx]
        option_type = ticker[idx_num1 + idx_num2.start() + 1 + 7 :][
            strike_idx : strike_idx + 2
        ]
        return [symbol, expiry, strike_price, option_type]
    idx_num = re.search(r"\d", ticker).start()
    symbol = ticker[:idx_num]
    expiry = ticker[idx_num : idx_num + 7]
    strike_idx = re.search(r"\D", ticker[idx_num + 7 :]).start()
    strike_price = ticker[idx_num + 7 :][:strike_idx]
    option_type = ticker[idx_num + 7 :][strike_idx : strike_idx + 2]
    return [symbol, expiry, strike_price, option_type]


def process_options_data(df_options: pd.DataFrame) -> pd.DataFrame:
    try:

        df_options["Symbol"], df_options["Expiry"], df_options[
            "Strike_Price"
        ], df_options["Option_Type"] = zip(
            *df_options["Ticker"].apply(lambda x: calculate_options_parameters(x))
        )
        df_options["DateTime"] = df_options["Date"] + " " + df_options["Time"]
        #   df_options["Expiry"] = df_options["Ticker"].apply(lambda x : x[idx_num:idx_num + 7])
        #   df_options["Strike_Price"] = df_options["Ticker"].apply(lambda x : x[idx_num + 7:][:strike_idx])
        #   df_options["Option_Type"] = df_options["Ticker"].apply(lambda x : x[idx_num + 7:][strike_idx:strike_idx + 2])
        return df_options
    except Exception:
        error_info = sys.exc_info()
        print("Error occured while preparing futures data: {}".format(error_info[1]))


if __name__ == "__main__":
    # establishing the connection

    engine = create_engine("postgresql://postgres:admin@localhost:5432/mydatabase")
    conn = psycopg2.connect(
        database="FNO", user="postgres", password="admin", host="127.0.0.1", port="5432"
    )
    conn.autocommit = True
    # print(os.getcwd())
    df = pd.read_csv(
        os.path.join(
            os.path.dirname((os.path.realpath(__file__))), Path("Data\\NFO.csv")
        )
    )
    df.drop(
        columns=[
            "Unnamed: 9",
            "OUTPUT>>>",
            "INDEX",
            "Ticker.1",
            "Date.1",
            "Time.1",
            "Open.1",
            "High.1",
            "Low.1",
            "Close.1",
            "Volume.1",
            "Open Interest.1",
            "SYMBOL",
            "EXPIRY",
            "STRIKE",
            "TYPE",
        ],
        inplace=True,
    )
    pattern = re.compile(r"(-I|-II|-III)", re.I)
    df_futures_raw = df[df["Ticker"].str.contains(pattern)]
    if df_futures_raw.shape[0] > 0:
        df_futures_processed = process_futures_data(df_futures_raw)
    df_options_raw = df[df["Ticker"].str.contains("PE.NFO|CE.NFO")]
    if df_options_raw.shape[0] > 0:
        df_options_processed = process_options_data(df_options_raw)
        df_options_processed["Expiry"] = pd.to_datetime(
            df_options_processed["Expiry"], format="%d%b%y"
        )
        df_options_processed["Week_Month"] = df_options_processed[
            "Expiry"
        ].dt.date.apply(lambda x: week_number_of_month(x))
        df_options_processed["Month_End"] = df_options_processed["Expiry"].apply(
            lambda x: pd.Period(x, freq="M").end_time.date()
        )
        df_options_processed["Month_End"] = pd.to_datetime(
            df_options_processed["Month_End"], format="%Y-%m-%d"
        )
        df_options_processed["Day_Month"] = df_options_processed["Expiry"].dt.day
        df_options_processed["Day_Month_End"] = df_options_processed["Month_End"].dt.day
        df_options_processed["Days_Diff"] = (
            df_options_processed["Day_Month_End"] - df_options_processed["Day_Month"]
        )
        df_options_processed["Expiry_Type"] = np.where(
            df_options_processed["Days_Diff"] <= 7, "Monthly", "Weekly"
        )
        df_options_processed["Expiry"] = df_options_processed["Expiry"].astype(str)
        df_options_processed.drop(
            columns=[
                "Month_End",
                "Day_Month",
                "Day_Month_End",
                "Days_Diff",
                "Week_Month",
            ],
            inplace=True,
        )
        df_options_processed["Future_Series"] = np.NaN
        df_options_processed = df_options_processed[
            [
                "Ticker",
                "Date",
                "Time",
                "DateTime",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Open Interest",
                "Symbol",
                "Future_Series",
                "Expiry",
                "Strike_Price",
                "Option_Type",
                "Expiry_Type",
            ]
        ]
        df_futures_processed = df_futures_processed[
            [
                "Ticker",
                "Date",
                "Time",
                "DateTime",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Open Interest",
                "Symbol",
                "Future_Series",
                "Expiry",
                "Strike_Price",
                "Option_Type",
                "Expiry_Type",
            ]
        ]
        df_final = pd.concat([df_futures_processed, df_options_processed], axis=0)
        df_final.rename(
            columns={
                "Date": "Created_Date",
                "Time": "Created_Time",
                "DateTime": "Created_Date_Time",
                "Open": "Open_Price",
                "High": "High_Price",
                "Low": "Low_Price",
                "Close": "Close_Price",
                "Open Interest": "Open_Interest",
            },
            inplace=True,
        )
        df_final.to_sql("fno_data", conn, if_exists="append")

    print("Process completed")
    # df_futures_processed.to_csv("Futures.csv",index=False)
    # df_options_processed.to_csv("Options.csv",index=False)
