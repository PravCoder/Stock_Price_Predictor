from dotenv import load_dotenv
load_dotenv()
import hopsworks
import pandas as pd
import numpy as np
import config as config  # not doing src.config
from datetime import datetime, timedelta

from data import (
    add_new_features,
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

def get_model_predictions(model, features):
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

def load_predictions_from_store():
    pass




def get_previous_rows(prices, cur_date, n_previous=12):
    # Find the index of cur_date
    cur_index = prices[prices["datetime"] == cur_date].index[0]
    
    # Get the previous n_previous rows
    previous_rows = prices.iloc[cur_index - n_previous:cur_index]
    return previous_rows
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

            "SMA_5": np.nan,
            "SMA_20": np.nan,
            "EMA_5": np.nan,
            "EMA_20": np.nan,
            "vol_5": np.nan,
            "daily_return": np.nan,
            "price_diff": np.nan,
            "volume_sma_5": np.nan,
            "volume_change": np.nan
        })
    # future_dates
    prices = pd.concat([prices, future_dates_df]).drop_duplicates(subset=['datetime']).sort_values(by='datetime').reset_index(drop=True)

    n_previous_days = 12
    step_size = 1

    for cur_date in future_date_range:
        cur_date_row = pd.DataFrame(prices[prices["datetime"] == cur_date])["datetime"]
        previous_rows = get_previous_rows(prices, cur_date, n_previous_days)


        features = transform_single_example_into_features(previous_rows) 
        preds = model.predict(features)
        print(f"Preds: {preds}")

def transform_single_example_into_features(example):
    print(example)
    features = []
    # convert feature values to numerical only
    feature_values = example.apply(pd.to_numeric, errors='coerce')
    # add the days-rows of cur indx-pair to features which is an example
    features.append(feature_values.values)

    features = np.array(features, dtype=np.float32)
    # flatten into 2D since 3-dimensional cannot we fed into model 
    # features = [d1f1, d1f2, d1f3, d2f1, d2f2, d2f3]
    n_previous_days = features.shape[1]
    n_features = features.shape[2]
    features = features.reshape(features.shape[0], n_previous_days * n_features)
    return features

# get future feature data:
# - current date to 30 days after
# - estimate features for each day?

# get model predictions of future data
# input into the model for day_x in the previous N days and features. 
# so predict day one and add it to the data, predict dat two adn add it to the data. 