import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(layout="wide")

st.title("NYC Taxi Travel Time Map")

# Add a loading message
with st.spinner('Loading map data...'):
    # Load the HTML content from the file
    with open('db_nyc_buildings_taxi_kepler_mapper_dow.html', 'rb') as file:
        html = file.read().decode('utf-8')
    
    # Display the map using the components interface
    components.html(html, height=800)

# Add some explanatory text
st.markdown("""
This map shows taxi pickup patterns across different days of the week in New York City.
- Each building is colored based on the number of taxi pickups in its area
- Use the layer selector to view different days of the week
- Click on buildings to see detailed pickup counts
""")