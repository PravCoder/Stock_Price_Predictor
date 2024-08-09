# Also considered inference pipeline
from dotenv import load_dotenv
load_dotenv()
import hopsworks
import pandas as pd
import numpy as np
import config as config # not doing src.config
from datetime import datetime, timedelta

from data import (
    add_new_features,
    interpolate_backfill_frontfill,
    transform_ts_data_into_features_target
)

def get_hopsworks_project():
    # login to hopsworks using project name/api key
    return hopsworks.login(
        project=config.HOPSWORKS_PROJECT_NAME,
        api_key_value=config.HOPSWORKS_API_KEY
    ) 

def get_feature_store():
    project = get_hopsworks_project()  # return pointer to feature store
    return project.get_feature_store()

def get_model_predictions(model, features, ts_prices):  # gets predictions on historical data
    print("FEATURES")
    print(features.shape)
    X  = features
    # single_example = X[0].reshape(1, -1)
    # print(model.predict(single_example))
    predictions = []
    for input_seq in X:
        pred = model.predict(input_seq.reshape(1, -1)) 
        predictions.append(pred.flatten())

    results = pd.DataFrame(predictions, columns=["predicted_prices"])
    # results["datetime"] = ts_prices["datetime"] # adding datetime column to each predicted-price so we know for what day that prediction is for
    # print(results)
    return results

def load_batch_of_features_from_store(): # 2024-08-05
    feature_store = get_feature_store()
    n_features = config.N_FEATURES
    # get current date
    end_date = datetime.now().date().strftime('%Y-%m-%d')  # current date.

    end_date_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    start_date_datetime = end_date_datetime - timedelta(days=2*365)

    start_date = start_date_datetime.strftime('%Y-%m-%d')

    # get feature-view
    feature_view = feature_store.get_feature_view(
        name=config.FEATURE_VIEW_NAME,
        version=config.FEATURE_VIEW_VERSION
    )

    """# get specific batch of data betweens start-end date from feature-view
    ts_prices = feature_view.get_batch_data(
        start_time=(start_date_datetime - timedelta(days=1)),
        end_time=(end_date_datetime - timedelta(days=1))
    )"""
    ts_prices, _ = feature_view.training_data(
        description="Time-series daily stock prices"
    )
    ts_prices.sort_values(by=["datetime"], inplace=True)

    # return features
    return ts_prices

def load_model_from_registry():
    import joblib
    from pathlib import Path

    project = get_hopsworks_project()
    model_registry = project.get_model_registry()

    model = model_registry.get_model(
        name=config.MODEL_NAME,
        version=config.MODEL_VERSION,
    )

    model_dir = model.download()
    
    model_path = f"{model_dir}/lgb_model3.pkl"

    lgb_model = joblib.load(model_path)

    return lgb_model





# FUTURE PREDICTIONS:
def get_previous_rows(prices, cur_date, n_previous=12):
    # Find the index of cur_date
    cur_index = prices[prices["datetime"] == cur_date].index[0]
    
    # Get the previous n_previous rows
    previous_rows = prices.iloc[cur_index - n_previous:cur_index+1]  # TBD***: ADDING ONE HERE AS A TEMPORARY FIX TO GET 13-SEQUENCE LENGTH EVEN THOUGH ITS SUPPOSED TO BE 12
    return previous_rows

def transform_single_example_into_features(example_prices): # given dataframe of days which is perceived as a single sequence transform that into features/targets
    features = []
    cur_seq_days_matrix = []
    for _, day_row in example_prices.iterrows():  # iterate through every day in example-sequence
        values = list(day_row.drop(labels="datetime").values)  # get the values for cur-day and add it to the 2d-matrix where each row represents data for that day
        cur_seq_days_matrix.append(values)

    features.append(cur_seq_days_matrix)  # add the only sequence-matrix to features

    features = np.array(features, dtype=np.float32)
    return features  # there is no targets because its only used for future data

def get_future_predictions(num_days, prices, model):  # ts-data
    prices["datetime"] = pd.to_datetime(prices["datetime"]).dt.tz_localize(None)

    # Get the last date in the DataFrame
    last_date_in_df = prices["datetime"].max()
    
    # Generate future dates
    future_dates = [(last_date_in_df + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, num_days + 1)]
    future_date_range = pd.date_range(start=future_dates[0], end=future_dates[-1], freq='D')

    future_dates_df = pd.DataFrame({
            'datetime': future_date_range,  # pass missing dates
            'open_price': np.nan,
            'high_price': np.nan,
            'low_price': np.nan,
            'close_price': np.nan,
            'volume': np.nan,
            'vw_avr_price': np.nan,
            'num_transactions': np.nan,

            "sma_5": np.nan,
            "sma_20": np.nan,
            "ema_5": np.nan,
            "ema_20": np.nan,
            "vol_5": np.nan,
            "daily_return": np.nan,
            "price_diff": np.nan,
            "volume_sma_5": np.nan,
            "volume_change": np.nan
        })
    prices.columns = prices.columns.str.lower() # chagnge all col names to lowercase
    future_dates_df.columns = future_dates_df.columns.str.lower()
    # future_dates
    prices = pd.concat([prices, future_dates_df]).drop_duplicates(subset=['datetime']).sort_values(by='datetime').reset_index(drop=True)

    n_previous_days = 12
    step_size = 1

    predictions = []
    # iterate through every date in future-date
    for cur_date in future_date_range:
        cur_date_row = pd.DataFrame(prices[prices["datetime"] == cur_date])  # get the future-date row in prices-df
        previous_rows = get_previous_rows(prices, cur_date, n_previous_days)  # get the previous N-rows which are features in prices-df to cur-date-row
        
        # print(f"\nCur date: {cur_date}")
        # print(prices)
        
        features = transform_single_example_into_features(previous_rows)  # this features only contains one sequence
        # print(f"\nFeatures: {features.shape}")

        # from the features get its only example-sequence and reshape to be fed
        single_example = features[0].reshape(1, -1) 
        preds = model.predict(single_example)  # compute predictions with single-example-sequence
        predictions.append(preds.flatten())   # flatten preds which is just [175.3] beacause only one prediction 1 output node
        # print(f"Preds: {preds}")
        # print("----------------------------------------")

        # for cur-future-date-row set close-price-col is what we predicted because its in the future
        cur_date_row["close_price"] = preds.flatten()
        # add cur-date-row-future that we predicted to the prices-df
        prices = pd.concat([prices, cur_date_row]).drop_duplicates(subset=['datetime']).sort_values(by='datetime').reset_index(drop=True)
        # and compute that days features and interpolate missing features
        prices = add_new_features(prices)
        prices = interpolate_backfill_frontfill(prices)

    results = pd.DataFrame(predictions, columns=["future_predicted_prices"])
    results["datetime"] = future_date_range  # return df with 2-cols price and date of future dates.
    return results

# get future feature data:
# - current date to 30 days after
# - estimate features for each day?

# get model predictions of future data
# input into the model for day_x in the previous N days and features. 
# so predict day one and add it to the data, predict dat two adn add it to the data. 



def load_historical_predictions_from_store():
    feature_store = get_feature_store()

    # Retrieve the feature group
    feature_group = feature_store.get_feature_group(
        name="model_prediction_historical",
        version=1
    )

    # Read the data from the feature group
    # historical_predictions_df = feature_group.read().sort_values(by='datetime').reset_index(drop=True)
    historical_predictions_df = feature_group.read()
    return historical_predictions_df

def load_future_predictions_from_store():
    feature_store = get_feature_store()

    # Retrieve the feature group
    feature_group = feature_store.get_feature_group(
        name="model_prediction_future",
        version=1
    )

    # Read the data from the feature group
    future_predictions_df = feature_group.read().sort_values(by='datetime').reset_index(drop=True)
    future_predictions_df  # make sure to sort
    return future_predictions_df