import os
from dotenv import load_dotenv
load_dotenv()



HOPSWORKS_PROJECT_NAME = "taxi_demand_pravachan"
try:
    HOPSWORKS_API_KEY = os.environ["HOPSWORKS_API_KEY"]  # extract the api-key-var from the file
except:
    raise Exception("Create an .env file on projedt root with the hopsworks-api-key")

FEATURE_GROUP_NAME = "time_series_daily_feature_group"
FEATURE_GROUP_VERSION =  3 

FEATURE_VIEW_NAME = "time_series_daily_feature_view"
FEATURE_VIEW_VERSION = 1
N_FEATURES = 12

MODEL_NAME = "lightgbm_model_stock_prices"
MODEL_VERSION = 2