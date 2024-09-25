import json
import argparse
import hashlib
import pandas as pd
def check_psql_credentials(cred_str):
    required_keys = ['db_name', 'db_user', 'db_password']
    try:
        credentials = json.loads(cred_str)
        if not all(key in credentials for key in required_keys):
            raise ValueError("Missing required keys: {}".format(required_keys))
    except (json.JSONDecodeError, ValueError) as e:
        raise argparse.ArgumentTypeError(f"Invalid psql_credentials: {e}")
    return credentials


def get_country_short_for_exchange(psql_client, is_test_mode, where_condition=""):
    exchange_code_countries = psql_client.read_from_psql_and_return_json_list("v_map_exchange_code_country_new",
                                                              is_test_mode=is_test_mode,
                                                              where_condition=where_condition,
                                                              table_with_json_data=True, rename_columns=False)

    map_exchange_to_countries = {}
    for exchange_code_country in exchange_code_countries:
        if exchange_code_country["country_iso2"]:
            map_exchange_to_countries[exchange_code_country["exchange_code"]] = exchange_code_country["country_iso2"].lower()
        else:
            print("no country iso2 found.")

    return map_exchange_to_countries


def get_country_short_for_full_name(psql_client, is_test_mode, where_condition=""):
    countries_full_to_short = psql_client.read_from_psql_and_return_json_list("country_codes_iso",
                                                              is_test_mode=is_test_mode,
                                                              where_condition=where_condition,
                                                              table_with_json_data=True, rename_columns=False)

    map_full_name_to_short = {}
    for country_full_to_short in countries_full_to_short:
        if "," in country_full_to_short["country_full"]:
            country_full = country_full_to_short["country_full"].split(",")[0]
        else:
            country_full = country_full_to_short["country_full"]
        map_full_name_to_short[country_full] = country_full_to_short["country_short"].lower()
    return map_full_name_to_short


def normalize_country_name(country_name):
    if country_name in ["US", "USA", "America"]:
        normalized_country_name = "United States"
    elif country_name == "South Korea":
        normalized_country_name = "Korea"
    else:
        normalized_country_name = country_name
    return normalized_country_name


import warnings
from cryptography.utils import CryptographyDeprecationWarning
warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)

def get_keys_for_hash(table_name):
    if table_name in ("eodhd_assets", "leeway_assets", "leeway_financials", "eodhd_financial", "fmp_commodities"):
        keys_for_hash = ['ticker', 'exchange_code']

    elif table_name == "frontend_screeners_combined":
        keys_for_hash = ['ticker', 'screener_name', 'last_quarter', 'last_year', 'company_name']

    elif table_name == "seo_articles":
        keys_for_hash = ['signal_date', 'signal_type', 'signal', 'company_name']

    elif table_name == "frontend_insider":
        keys_for_hash = ['flag', 'country', 'company', 'patterns', 'chart_link', 'transaction_day']

    elif table_name == "finviz":
        keys_for_hash = ['No', 'Ticker', 'Company', 'Sector', 'Industry', 'Country', 'Market Cap', 'P/E', 'Price', 'Change', 'Volume', 'color', 'visited', 'chart_link', 'pattern_name', 'resolution', 'exchange_code']

    elif table_name == "finnhub":
        keys_for_hash = ['aprice', 'atime', 'bprice', 'btime', 'cprice', 'ctime', 'dprice', 'dtime', 'end_price', 'end_time', 'entry', 'eprice', 'etime', 'mature', 'patternname', 'patterntype', 'profit1', 'profit2', 'sortTime', 'start_price', 'start_time', 'status', 'stoploss', 'symbol', 'terminal', 'Industry', 'Market Cap', 'color', 'visited', 'resolution']

    elif table_name == "insider":
        keys_for_hash = ['Notification date', 'Transaction date', 'Company', 'Chart link', 'Flag link', 'Transaction type', 'Insider', 'Position', 'Number of shares', 'Price', 'Total value']

    elif table_name == "ohlc":
        keys_for_hash = ["ticker", "exchange_code", "data_source", "data_day"]

    elif table_name == "ohlc_patterns":
        keys_for_hash = ["ticker", "data_source", "pattern", "data_day"]

    elif table_name == "finnhub_assets":
        keys_for_hash = ["symbol", "exchange_code"]
        
    return keys_for_hash


def convert_dataframe_to_list(data: pd.DataFrame) -> list:
    data_list = data.to_dict('records')
    return data_list
def convert_list_to_dataframe(data: list) -> pd.DataFrame:
    df =  pd.DataFrame(data)
    return df



def check_if_keys_for_hash_exist(input_data: dict, keys_for_hash: list):
    number_of_keys_is = len([value for value in keys_for_hash if value in input_data.keys()])
    number_of_keys_should = len(keys_for_hash)
    #print("Elements in 'number_of_keys_is' but not in 'number_of_keys_should':", set([value for value in keys_for_hash if value in input_data.keys()]) - set(keys_for_hash))
    #print("Elements in 'number_of_keys_should' but not in 'number_of_keys_is':", set(keys_for_hash) - set([value for value in keys_for_hash if value in input_data.keys()]))
    assert number_of_keys_is == number_of_keys_should, ("Number of defined keys for hash != Number of keys available. Maybe one of the keys is not in the data or the name is wrong", print(number_of_keys_is, number_of_keys_should))


def consistent_numeric_hash(input_data: dict, table_name: str, rehash=False) -> str:
    """
    Convert the input data for _id column to bytes if it's not already in byte format
    """
    keys_for_hash = get_keys_for_hash(table_name)

    if table_name in ['finviz',]:
        string_for_hash = ''.join(str(input_data[key]) for key in keys_for_hash if key in input_data and input_data[key] != None)
    elif table_name in ['frontend_screeners_combined']:
         
        string_for_hash = ''
        for key in keys_for_hash:
            if key in input_data:
                if key == 'company_name':
                    key_for_hash = str(input_data[key]).split()[0].lower()
                    string_for_hash += str(key_for_hash)
                else:
                    string_for_hash += str(input_data[key])

    else:
        string_for_hash = ''.join(str(input_data[key]) for key in keys_for_hash if key in input_data) 

    check_if_keys_for_hash_exist(input_data, keys_for_hash)


    if not isinstance(string_for_hash, bytes):
        input_data = str(string_for_hash).encode()
    hash_obj = hashlib.sha256()
    hash_obj.update(input_data)
    numeric_hash = int(hash_obj.hexdigest(), 16)
    numeric_hash = numeric_hash % (1 << 32)

    if rehash:
        return numeric_hash, keys_for_hash, string_for_hash
    else:
        return numeric_hash