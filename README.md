# NYC Taxi Travel Time Visualization

This is a small project to help me get more hands on experience with DuckDb when working geospatial data.

This project visualizes taxi pickup patterns across different days of the week in New York City using KeplerGL and Streamlit.


## Usage

### Generate the KeplerGL Map

1. Run the tlc_mapper.py script to generate the KeplerGL map:
    ```sh
    python backend/tlc_mapper.py
    ```

2. This will create an HTML file db_nyc_buildings_taxi_kepler_mapper_dow.html
### Run the Streamlit App

1. Start the Streamlit app:
    ```sh
    streamlit run streamlit_app.py
    ```

2. Open your web browser and navigate to `http://localhost:8501` to view the app.


