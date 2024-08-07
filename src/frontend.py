import requests
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# plotting libraries
import streamlit as st
import geopandas as gpd
import pydeck as pdk
import pytz
# inference pipeline functions
from inference import (
    # load_predictions_from_store,
    load_batch_of_features_from_store,
    load_model_from_registry,
    get_model_predictions
)
from data import (
    transform_ts_data_into_features_target
)

# set title
st.set_page_config(layout="wide")
st.title(f"Stock Price Predictor")
current_date = datetime.now().date().strftime('%Y-%m-%d') 
st.header(f"{current_date}")

# set progress side bar
progress_bar = st.sidebar.header("⚙️ Working Progress")
progress_bar = st.sidebar.progress(0)
N_STEPS = 7

with st.spinner(text="Fetching batch of inference data"):
    # features-df = get most recent features stored by feature-pipeline from feature-store
    ts_prices = load_batch_of_features_from_store()
    st.sidebar.write('✅ Model predictions arrived') 
    progress_bar.progress(2/N_STEPS)
    print(f"{ts_prices}")


with st.spinner(text="Loading ML model from model registry"):
    model = load_model_from_registry()  
    st.sidebar.write("✅ ML model was loaded from the registry") 
    progress_bar.progress(3/N_STEPS)

with st.spinner(text="Computing Model predictions"):
    n_previous_days = 12
    step_size = 1
    features, targets = transform_ts_data_into_features_target(ts_prices, n_previous_days, step_size) # convert ts-data from feature-store into features/targets for training
    results = get_model_predictions(model, features)  # predictions
    st.sidebar.write("✅ Model predictions arrived") 
    progress_bar.progress(4/N_STEPS)
    print(f"Model predictions {results}")