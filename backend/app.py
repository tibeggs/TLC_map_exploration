import streamlit as st
from keplergl import KeplerGl
from tlc_mapper import create_travel_time_map
import pandas as pd

# Streamlit UI
st.set_page_config(page_title="NYC Taxi Travel Time Map", layout="wide")
st.title("NYC Taxi Travel Time Visualization")

# Initialize session state
if 'data' not in st.session_state:
    st.session_state.data = None
if 'config' not in st.session_state:
    st.session_state.config = None

# Load data if needed
if st.session_state.data is None:
    try:
        df, config = create_travel_time_map()
        st.session_state.data = df
        st.session_state.config = config
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.stop()

# Add controls
st.sidebar.header("Map Controls")

# Day of week selector
days_mapping = {
    0: 'Monday',
    1: 'Tuesday',
    2: 'Wednesday',
    3: 'Thursday',
    4: 'Friday',
    5: 'Saturday',
    6: 'Sunday'
}

# Color scheme selector
color_scheme = st.sidebar.selectbox(
    "Color Scheme",
    ["Global Warming", "Viridis", "Plasma", "Inferno", "Magma"]
)

# Weight scale selector
weight_scale = st.sidebar.selectbox(
    "Weight Scale",
    ["linear", "logarithmic", "sqrt"]
)

# Create/update map
if st.sidebar.button("Update Map") or 'map' not in st.session_state:
    # Get a fresh config and update it with user selections
    
    # Create map with updated config
    map_1 = KeplerGl(
        height=600,
        config=config,
        center_map=True,
    )
    
    # Add the data
    map_1.add_data(data=st.session_state.data, name="travel_time_data")
    
    # Update the visualization settings after adding data
    map_1.config['visState']['layers'][0]['config']['colorRange']['name'] = color_scheme
    map_1.config['visState']['layers'][0]['visualChannels']['weightScale'] = weight_scale
    
    st.session_state.map = map_1

# Display the map if it exists
if 'map' in st.session_state:
    st.components.v1.html(st.session_state.map._repr_html_(), height=600)
else:
    st.warning("No data available. Please check the data loading and try again.") 