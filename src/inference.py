import hopsworks
import pandas as pd
import numpy as np
import src.config as config
from datetime import datetime, timedelta


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
    predictions = model.predict(features)

    results = pd.DataFrame()
    results["predicted_prices"] = predictions.round(0)
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

    # get specific batch of data betweens start-end date from feature-view
    ts_prices = feature_view.get_batch_data(
        start_time=(start_date_datetime - timedelta(days=1)),
        end_time=(end_date_datetime - timedelta(days=1))
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
    
    model_path = f"{model_dir}/lgb_model.pkl"

    lgb_model = joblib.load(model_path)

    return lgb_model