import streamlit as st
import streamlit.components.v1 as components
from backend.tlc_mapper import create_travel_time_map, create_kepler_map

st.set_page_config(layout="wide")

st.title("NYC Taxi Travel Time Map")

# Add a loading message
with st.spinner('Loading map data...'):
    # Load the data
    gdf_joined = create_travel_time_map()
    
    # Sample the data (optional, remove if you want to show all data)
    gdf_joined = gdf_joined.sample(10000)
    
    # Create the Kepler map
    map_1 = create_kepler_map(gdf_joined)
    
    # Get the HTML content
    html = map_1._repr_html_()
    
    # Display the map using the components interface
    components.html(html, height=800)

# Add some explanatory text
st.markdown("""
This map shows taxi pickup patterns across different days of the week in New York City.
- Each building is colored based on the number of taxi pickups in its area
- Use the layer selector to view different days of the week
- Click on buildings to see detailed pickup counts
""") 