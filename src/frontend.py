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
import plotly.express as px

# inference pipeline functions
from inference import (
    # load_predictions_from_store,
    load_batch_of_features_from_store,
    load_model_from_registry,
    get_model_predictions,
    get_future_predictions
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
    # get historical 2 years back frmo current date
    ts_prices = load_batch_of_features_from_store()
    # TBD: get future data from current date 

    st.sidebar.write('✅ Model predictions arrived') 
    progress_bar.progress(2/N_STEPS)
    # print(f"{ts_prices}")


with st.spinner(text="Loading ML model from model registry"):
    model = load_model_from_registry()  
    st.sidebar.write("✅ ML model was loaded from the registry") 
    progress_bar.progress(3/N_STEPS)

with st.spinner(text="Computing Model predictions"):
    n_previous_days = 12
    step_size = 1
    features, targets = transform_ts_data_into_features_target(ts_prices, n_previous_days, step_size) # convert ts-data from feature-store into features/targets for training
    # get historical prediction
    predictions = get_model_predictions(model, features)  # predictions

    # get future predictions
    st.sidebar.write("✅ Model predictions arrived") 
    progress_bar.progress(4/N_STEPS)
    print(f"Model predictions {predictions}")
    


# Get the last 30 days of actual close prices and plot against predictions
num_days_to_display = 30  # Change this value to zoom in/out
close_prices = ts_prices["close_price"].values[-num_days_to_display:]  # Ensure the lengths match

# Ensure the lengths match for the last num_days_to_display days
predicted_prices = predictions["predicted_prices"][-num_days_to_display:]
dates = ts_prices["datetime"].values[-num_days_to_display:]

# dataframe for predictions
results_df = pd.DataFrame({
    "Date": dates,
    "Actual": close_prices,
    "Predicted": predicted_prices
})

# Display the chart title
st.subheader(f"Actual vs Predicted Stock Prices in last {num_days_to_display} days")

# Plot the actual and predicted prices using plotly
fig = px.line(results_df, x="Date", y=["Actual", "Predicted"], title="Actual vs Predicted Stock Prices")
fig.update_yaxes(range=[min(close_prices) - 10, max(close_prices) + 10])  # Set the y-axis range

st.plotly_chart(fig)

num_days = 5
get_future_predictions(num_days, ts_prices, model)