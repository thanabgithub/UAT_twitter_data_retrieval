# %%
from typing import Dict, Any

import pandas as pd
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict

load_dotenv()
bearer_token = os.environ.get("BEARER_TOKEN")


# %%

def get_trends(url):
    """
    ================
    TWITTER: GET trends/place
    ================
    Returns the top 50 trending topics for a specific id, if trending information is available for it.
    Note: The id parameter for this endpoint is the "where on earth identifier" or WOEID

    # https://developer.twitter.com/en/docs/twitter-api/v1/trends/trends-for-location/api-reference/get-trends-place

    """

    def bearer_oauth(r):
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = f"Bearer {bearer_token}"
        return r

    def connect_to_endpoint(WOEID: str):
        if not isinstance(WOEID, str):
            WOEID = str(WOEID)
        url = "https://api.twitter.com/1.1/trends/place.json?id=" + WOEID
        response = requests.get(url, auth=bearer_oauth)
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    return connect_to_endpoint(url)


def get_woeid_trends():
    available_data_to_woeid = pd.read_excel("app/data/ELT_JP_WOEID.xlsx", index_col='Area').astype("string")
    available_data_to_woeid = available_data_to_woeid.to_dict('dict')['WOEID']

    woeid_trends: Dict[str, str] = {}

    for area, woeid_str in available_data_to_woeid.items():
        print(area + ": " + woeid_str)
        json_response = get_trends(woeid_str)
        woeid_trends[area] = json_response

    return woeid_trends


def clean_woeid_trends(woeid_trends):
    """
    ================
    convert from noSQL to dataframe and prepare for further data cleansing
    ================
    Returns dataframe, Japan trendings keywords (list), as_of as timestep
    """
    wip_woeid_trends_df = pd.DataFrame()
    national_trends = []
    as_of = []
    for area in woeid_trends.keys():

        toptrending_all_areas = woeid_trends[area][0]['trends']
        SQLite_timeformatted = woeid_trends[area][0]['as_of'].replace('T', ' ').replace('Z', '')
        as_of.append(SQLite_timeformatted)
        row = []
        for toptrending_all_each_area in toptrending_all_areas:
            keyword = toptrending_all_each_area['name']
            row.append(keyword)
            if area == "日本":
                national_trends.append(keyword)
        validate = 50 - len(row)
        if validate > 0:
            empty_lst = [''] * validate
            row.extend(empty_lst)
        elif validate < 0:
            row = row[:50]
        wip_woeid_trends_df[area] = row
    return wip_woeid_trends_df, national_trends, as_of


def finalize_woeid_trends_sql_format(as_of: list, wip_woeid_trends_df: pd.DataFrame()) -> pd.DataFrame():
    ## Clean data format preparing to insert to database

    sq_lite_timeformatted_now: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sq_lite_timeformatted_now_lst = [sq_lite_timeformatted_now] * len(as_of)

    temp_datetime = pd.DataFrame([sq_lite_timeformatted_now_lst, as_of]).T
    fin_woeid_trends_df = pd.concat([temp_datetime, wip_woeid_trends_df.T.reset_index()], axis=1, ignore_index=True)

    sql_col = ['ExtractDatetime', 'AsofDatetime', 'Area']
    temp_rank = [*("TOP_" + str(ele) for ele in [*range(1, 51)])]
    sql_col.extend(temp_rank)
    fin_woeid_trends_df.columns = sql_col
    fin_woeid_trends_df = fin_woeid_trends_df.astype("string")

    fin_woeid_trends_df.ExtractDatetime = pd.to_datetime(fin_woeid_trends_df.ExtractDatetime)
    fin_woeid_trends_df.AsofDatetime = pd.to_datetime(fin_woeid_trends_df.AsofDatetime)

    return fin_woeid_trends_df


def get_count_keyword_7_days(query_params):
    """
    ================
    TWITTER: GET /2/tweets/counts/recent
    ================
    Returns count of Tweets from the last seven days that match a query.


    # https://developer.twitter.com/en/docs/twitter-api/tweets/counts/api-reference/get-tweets-counts-recent

    """

    def bearer_oauth(r):
        r.headers["Authorization"] = f"Bearer {bearer_token}"
        return r

    def connect_to_endpoint(url, params):
        response = requests.get(url, auth=bearer_oauth, params=params)
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    endpoint_url = "https://api.twitter.com/2/tweets/counts/recent"

    return connect_to_endpoint(endpoint_url, query_params)


# %%

## Extract and unpack them from noSQL format to SQL format


def get_timeseries_trends(national_trends):
    import warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)

    wip_timeseries_trend_df = pd.DataFrame()
    my_validation = {}

    for keyword in national_trends:
        query_params = {'query': keyword}
        json_response = get_count_keyword_7_days(query_params)
        df = pd.DataFrame(json_response['data'])

        df.drop("start", axis=1, inplace=True)
        df.end = df.end.str.replace('T', ' ')
        df.end = df.end.str.replace('.000Z', '')

        df_kw = pd.concat([pd.DataFrame(columns=['keyword'], data=[keyword] * len(df)), df], axis=1, ignore_index=True)
        wip_timeseries_trend_df = wip_timeseries_trend_df.append(df_kw)
        my_validation[keyword] = df

    return wip_timeseries_trend_df, my_validation


def finalize_timeseries_trend_sql_format(wip_timeseries_trend_df):
    sq_lite_timeformatted_now: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sq_lite_timeformatted_now_lst = [sq_lite_timeformatted_now] * len(wip_timeseries_trend_df)
    temp = pd.DataFrame(sq_lite_timeformatted_now_lst)
    fin_timeseries_trend_df = pd.concat(
        [pd.DataFrame(sq_lite_timeformatted_now_lst), wip_timeseries_trend_df.reset_index(drop=True)], axis=1,
        ignore_index=True)

    fin_timeseries_trend_df.columns = ['ExtractDatetime', 'Keyword', 'HistoricalDatetime', 'Counts']

    fin_timeseries_trend_df.ExtractDatetime = pd.to_datetime(fin_timeseries_trend_df.ExtractDatetime)
    fin_timeseries_trend_df.HistoricalDatetime = pd.to_datetime(fin_timeseries_trend_df.HistoricalDatetime)
    fin_timeseries_trend_df.Keyword = fin_timeseries_trend_df.Keyword.astype('string')

    return fin_timeseries_trend_df


# %%



# validate data format in advance before inserting to sql

from sqlalchemy import create_engine
from sqlalchemy import types as sqltypes




# %%




# %%


# %%
# https://stackoverflow.com/questions/60523645/how-to-avoid-duplicates-on-copying-data-from-python-pandas-dataframe-to-sql-data
# Check duplicates

def insert_cross_section_trends(engine, fin_woeid_trends_df):

    def get_col_options_cross_sections():
        # https://stackoverflow.com/questions/60523645/how-to-avoid-duplicates-on-copying-data-from-python-pandas-dataframe-to-sql-data

        col_options = dict(
            dtype={
                'ExtractDatetime': sqltypes.DATETIME,
                'AsofDatetime': sqltypes.DATETIME,
                'Area': sqltypes.VARCHAR(length=20),
                'TOP_1': sqltypes.VARCHAR(length=20),
                'TOP_2': sqltypes.VARCHAR(length=20),
                'TOP_3': sqltypes.VARCHAR(length=20),
                'TOP_4': sqltypes.VARCHAR(length=20),
                'TOP_5': sqltypes.VARCHAR(length=20),
                'TOP_6': sqltypes.VARCHAR(length=20),
                'TOP_7': sqltypes.VARCHAR(length=20),
                'TOP_8': sqltypes.VARCHAR(length=20),
                'TOP_9': sqltypes.VARCHAR(length=20),
                'TOP_10': sqltypes.VARCHAR(length=20),
                'TOP_11': sqltypes.VARCHAR(length=20),
                'TOP_12': sqltypes.VARCHAR(length=20),
                'TOP_13': sqltypes.VARCHAR(length=20),
                'TOP_14': sqltypes.VARCHAR(length=20),
                'TOP_15': sqltypes.VARCHAR(length=20),
                'TOP_16': sqltypes.VARCHAR(length=20),
                'TOP_17': sqltypes.VARCHAR(length=20),
                'TOP_18': sqltypes.VARCHAR(length=20),
                'TOP_19': sqltypes.VARCHAR(length=20),
                'TOP_20': sqltypes.VARCHAR(length=20),
                'TOP_21': sqltypes.VARCHAR(length=20),
                'TOP_22': sqltypes.VARCHAR(length=20),
                'TOP_23': sqltypes.VARCHAR(length=20),
                'TOP_24': sqltypes.VARCHAR(length=20),
                'TOP_25': sqltypes.VARCHAR(length=20),
                'TOP_26': sqltypes.VARCHAR(length=20),
                'TOP_27': sqltypes.VARCHAR(length=20),
                'TOP_28': sqltypes.VARCHAR(length=20),
                'TOP_29': sqltypes.VARCHAR(length=20),
                'TOP_30': sqltypes.VARCHAR(length=20),
                'TOP_31': sqltypes.VARCHAR(length=20),
                'TOP_32': sqltypes.VARCHAR(length=20),
                'TOP_33': sqltypes.VARCHAR(length=20),
                'TOP_34': sqltypes.VARCHAR(length=20),
                'TOP_35': sqltypes.VARCHAR(length=20),
                'TOP_36': sqltypes.VARCHAR(length=20),
                'TOP_37': sqltypes.VARCHAR(length=20),
                'TOP_38': sqltypes.VARCHAR(length=20),
                'TOP_39': sqltypes.VARCHAR(length=20),
                'TOP_40': sqltypes.VARCHAR(length=20),
                'TOP_41': sqltypes.VARCHAR(length=20),
                'TOP_42': sqltypes.VARCHAR(length=20),
                'TOP_43': sqltypes.VARCHAR(length=20),
                'TOP_44': sqltypes.VARCHAR(length=20),
                'TOP_45': sqltypes.VARCHAR(length=20),
                'TOP_46': sqltypes.VARCHAR(length=20),
                'TOP_47': sqltypes.VARCHAR(length=20),
                'TOP_48': sqltypes.VARCHAR(length=20),
                'TOP_49': sqltypes.VARCHAR(length=20),
                'TOP_50': sqltypes.VARCHAR(length=20)
            }
        )

        return col_options

    col_options = get_col_options_cross_sections()

    fin_woeid_trends_df.to_sql(name="sqltable_temp", con=engine, if_exists='replace', index=False, **col_options)

    query = """
        SELECT ExtractDatetime, AsofDatetime, Area, TOP_1, TOP_2, TOP_3, TOP_4, TOP_5, TOP_6, TOP_7, TOP_8, TOP_9, TOP_10, TOP_11, TOP_12, TOP_13, TOP_14, TOP_15, TOP_16, TOP_17, TOP_18, TOP_19, TOP_20, TOP_21, TOP_22, TOP_23, TOP_24, TOP_25, TOP_26, TOP_27, TOP_28, TOP_29, TOP_30, TOP_31, TOP_32, TOP_33, TOP_34, TOP_35, TOP_36, TOP_37, TOP_38, TOP_39, TOP_40, TOP_41, TOP_42, TOP_43, TOP_44, TOP_45, TOP_46, TOP_47, TOP_48, TOP_49, TOP_50
     FROM sqltable_temp 
        EXCEPT 
        SELECT ExtractDatetime, AsofDatetime, Area, TOP_1, TOP_2, TOP_3, TOP_4, TOP_5, TOP_6, TOP_7, TOP_8, TOP_9, TOP_10, TOP_11, TOP_12, TOP_13, TOP_14, TOP_15, TOP_16, TOP_17, TOP_18, TOP_19, TOP_20, TOP_21, TOP_22, TOP_23, TOP_24, TOP_25, TOP_26, TOP_27, TOP_28, TOP_29, TOP_30, TOP_31, TOP_32, TOP_33, TOP_34, TOP_35, TOP_36, TOP_37, TOP_38, TOP_39, TOP_40, TOP_41, TOP_42, TOP_43, TOP_44, TOP_45, TOP_46, TOP_47, TOP_48, TOP_49, TOP_50
     FROM CrossSectionTrends;
    """

    new_entries = pd.read_sql(query, con=engine)

    new_entries.ExtractDatetime = pd.to_datetime(new_entries.ExtractDatetime)
    new_entries.AsofDatetime = pd.to_datetime(new_entries.AsofDatetime)

    new_entries.to_sql(
        name="CrossSectionTrends", con=engine, if_exists='append', index=False, **col_options)

    return engine.execute("DROP TABLE sqltable_temp;")



# %%

def insert_timeseries_trends(engine, fin_timeseries_trend_df):

    def get_col_options_time_series():
        col_options = dict(
            dtype={
                'ExtractDatetime': sqltypes.DATETIME,
                'Keyword': sqltypes.VARCHAR(length=20),
                'HistoricalDatetime': sqltypes.DATETIME,
                'Counts': sqltypes.INTEGER
            }
        )

        return col_options

    col_options = get_col_options_time_series()
    fin_timeseries_trend_df.to_sql(name="sqltable_temp", con=engine, if_exists='replace', index=False, **col_options)

    query = """
        SELECT ExtractDatetime, Keyword, HistoricalDatetime, Counts
     FROM sqltable_temp 
        EXCEPT 
        SELECT ExtractDatetime, Keyword, HistoricalDatetime, Counts
     FROM TimeSeriesTrends;
    """

    new_entries = pd.read_sql(query, con=engine)

    new_entries.ExtractDatetime = pd.to_datetime(new_entries.ExtractDatetime)
    new_entries.HistoricalDatetime = pd.to_datetime(new_entries.HistoricalDatetime)

    new_entries.to_sql(
        name="TimeSeriesTrends", con=engine, if_exists='append', index=False, **col_options)

    return engine.execute("DROP TABLE sqltable_temp;")



