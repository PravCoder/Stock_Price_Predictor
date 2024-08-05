import numpy as np
import pandas as pd
import requests
import os


def dowload_one_file_of_raw_data(symbol, start_date, end_date):
    POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    data = response.json()
    
    # Print keys to inspect the structure of the data
    print(dict(data).keys())

    # Extract the time series data
    time_series = data.get('results', [])
    
    # Check if time_series is empty or not
    if not time_series:
        print("No data available.")
        return

    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(time_series)
    
    # convert 't' (timestamp) to datetime and rename it to 'Date'
    df['datetime'] = pd.to_datetime(df['t'], unit='ms')
    # df.set_index('datetime', inplace=True)
    
    
    # drop the timestamp column 't'
    df.drop(columns=['t'], inplace=True)
    
    # rename columns for clarity
    df.rename(columns={
        'o': 'open_price',
        'h': 'high_price',
        'l': 'low_price',
        'c': 'close_price',
        'v': 'volume',
        'n': 'num_transactions',
        'vw': 'vw_avr_price'
    }, inplace=True)
    # print(df)
    

    path = f'../data/raw/prices_{start_date}-{end_date}.parquet'
    df.to_parquet(path, index=True)
    return df  # return raw data


def validate_raw_data(prices, start, end):
    start_date = pd.to_datetime(start)  # YYYY-MM-DD
    end_date = pd.to_datetime(end)

    prices = prices[prices.datetime >= start_date]
    prices = prices[prices.datetime < end_date]
    prices


# HANDLE MISSING DAYS IN TIME-SERIES DATA: INTERPOLATION
def interpolate_backfill_frontfill(prices):
    prices_interpolated = prices.interpolate(method='linear')  # average of back/front rows of null-row
    prices_interpolated.ffill(inplace=True)   # front-fll
    prices_interpolated.bfill(inplace=True)   # back-fill
    return prices_interpolated

def add_missing_days(prices, start_date, end_date):
    # convert datetime-column to datetime type and remove the time component
    prices['datetime'] = pd.to_datetime(prices['datetime'])

    # define date range
    # start_date = "2022-08-02"
    # end_date = "2024-08-02"

    full_date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # get the dates given from api, convert into comparable format
    existing_dates = pd.DatetimeIndex(prices['datetime']).normalize() 

    # Find missing dates
    missing_dates = full_date_range.difference(existing_dates)  # get the dates from the entire range that dont exist in existing-dates

    # create null dataframe for missing dates
    missing_dates_df = pd.DataFrame({
        'datetime': missing_dates,  # pass missing dates
        'open_price': np.nan,
        'high_price': np.nan,
        'low_price': np.nan,
        'close_price': np.nan,
        'volume': np.nan,
        'vw_avr_price': np.nan,
        'num_transactions': np.nan
    })


    # Concatenate and drop duplicates
    prices = pd.concat([prices, missing_dates_df]).drop_duplicates(subset=['datetime']).sort_values(by='datetime').reset_index(drop=True)
    prices_interpolated = interpolate_backfill_frontfill(prices)
    
    print("MISSING DATES")
    # missing_dates_df
    return prices, prices_interpolated


# RAW DATA TO TIME-SERIES DATA
def transform_raw_data_into_ts_data(prices, start_date, end_date):
    prices, prices_interpolated = add_missing_days(prices, start_date, end_date)
    ts_prices = add_new_features(prices_interpolated)
    return ts_prices

def add_new_features(prices_interpolated):
    # Simple moving average: average of prices over a specific period.
    prices_interpolated['SMA_5'] = prices_interpolated['close_price'].rolling(window=5).mean()
    prices_interpolated['SMA_20'] = prices_interpolated['close_price'].rolling(window=20).mean()

    # Exponential moving average: gives more weight to recent prices in an attempt to make them more responsive to new information
    prices_interpolated['EMA_5'] = prices_interpolated['close_price'].ewm(span=5, adjust=False).mean()
    prices_interpolated['EMA_20'] = prices_interpolated['close_price'].ewm(span=20, adjust=False).mean()

    # Volatility: standard deviation over a window to measure price fluctuation
    prices_interpolated['vol_5'] = prices_interpolated['close_price'].rolling(window=5).std()

    # Daily return: percent change from previous days closing price
    prices_interpolated['daily_return'] = prices_interpolated['close_price'].pct_change()

    # Price difference: difference between today's and yesterday's closing prices.
    prices_interpolated['price_diff'] = prices_interpolated['close_price'] - prices_interpolated['close_price'].shift(1)

    # Volume moving average: moving average of trading volume.
    prices_interpolated['volume_sma_5'] = prices_interpolated['volume'].rolling(window=5).mean()

    # Volume change: ercentage in trading volume
    prices_interpolated['volume_change'] = prices_interpolated['volume'].pct_change()



    # have to interpolated again because sometimes there is not enough previous days calcualte average of window so it becomes Nan, we interpolate again
    prices_interpolated = interpolate_backfill_frontfill(prices_interpolated) 
    return prices_interpolated


# TIME-SERIES DATA TO FEATURES/TARGETS
def transform_ts_data_into_features_target(prices, n_previous_days, step_size):
    # Get indices for slicing
    indicies = get_cutoff_indicies(prices, n_previous_days, step_size)

    # each element is an example, each example contains the previous-n-days, each day contains its features values
    features = []
    targets = []

    # iterate every indicies pair, number of indicies is number of examples
    for start_idx, end_idx in indicies:
        # extract feature values for previous days (d1, d2, ..., dn)
        # prices extract days-rows from start-indx of example to end-indx not inclusive because its the target, this is an example
        feature_values = prices.iloc[start_idx:end_idx]
        
        # convert feature values to numerical only
        feature_values = feature_values.apply(pd.to_numeric, errors='coerce')
        
        # add the days-rows of cur indx-pair to features which is an example
        features.append(feature_values.values)
        
        # extract target value for the next day which is close-price, the end-index of window
        target_value = prices.iloc[end_idx]['close_price']
        targets.append(target_value)  # add target value of example to targets

    # convert to numpy arrays
    features = np.array(features, dtype=np.float32)
    targets = np.array(targets, dtype=np.float32)

    # flatten into 2D since 3-dimensional cannot we fed into model 
    # features = [d1f1, d1f2, d1f3, d2f1, d2f2, d2f3]
    n_previous_days = features.shape[1]
    n_features = features.shape[2]
    features = features.reshape(features.shape[0], n_previous_days * n_features)

    return features, targets

def get_cutoff_indicies(data, n_previous_days, step_size):
    stop_position = len(data)-1

    first_indx = 0
    last_indx = n_previous_days+1  # target index
    indicies = []

    while last_indx <= stop_position:
        indicies.append((first_indx, last_indx))

        first_indx += step_size
        last_indx += step_size
    
    return indicies


# DATA SPLIT
from sklearn.model_selection import train_test_split

def train_test_split_tabular(tabular_prices, target_column_name="target_close_price_next_day", test_size=0.2, random_state=42):
    # seperate features/targets
    X = tabular_prices.drop(columns=[target_column_name])
    y = tabular_prices[target_column_name]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
    
    return X_train, X_test, y_train, y_test