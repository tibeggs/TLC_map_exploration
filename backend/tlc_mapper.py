import pandas as pd
import duckdb
import geopandas as gpd 
import plotly.express as px
import plotly.graph_objects as go
import os
import json
import shapely

def create_travel_time_map():
    """
    Create travel time map for all days of the week.
    Returns a DataFrame with data for all days.
    """
    # Generate unique filename
    output_data = f"travel_time_data_all_days.parquet"
    
    if os.path.exists(output_data):
        print(f"Loading existing data...")
        df = pd.read_parquet(output_data)
        return df
    
    print(f"Generating new map data...")
    con = duckdb.connect()
    print("Installing extensions...")
    con.sql(""" INSTALL h3 FROM community;
                LOAD h3;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_region='us-west-2';""")

    # Modify the URL based on the date
    url = f'./data/yellow_tripdata_2010-05.parquet'
    min_cnt = 20
    resolution = 10
    
    query = f"""
        SELECT DISTINCT
            h3_h3_to_string(h3_latlng_to_cell(pickup_latitude, pickup_longitude, {resolution})) AS cell_id,
            h3_cell_to_boundary_wkt(h3_h3_to_string(h3_latlng_to_cell(pickup_latitude, pickup_longitude, {resolution}))) AS boundary,
            pickup_datetime
        FROM read_parquet('{url}')
        WHERE 
            -- Filter for second week (days 8-14) of the month
            EXTRACT(DAY FROM CAST(pickup_datetime AS TIMESTAMP)) BETWEEN 8 AND 14
    """
    print("Initial query")
    df = con.sql(query).df()

    # Add temporal analysis using the existing df
    temporal_query = f"""
        SELECT 
            cell_id,
            CASE EXTRACT(DOW FROM CAST(pickup_datetime AS TIMESTAMP))
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END as day_of_week,
            COUNT(*) as trip_count
        FROM df
        GROUP BY 1, 2
        HAVING COUNT(*) >= {min_cnt}
        ORDER BY 
            cell_id,
            day_of_week
    """
    print("temporal_query")
    df_temporal = con.sql(temporal_query).df()
    print("Temporal analysis results:")
    print(df_temporal.head())

    gdf_cab = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    gdf_cab = gdf_cab.set_crs(epsg=4326, inplace=True)
    print(gdf_cab)

    bbox_string = "-74.27507,40.488386,-73.72905,40.957151" #NYC

    # Split the string into individual float values
    bbox_values = list(map(float, bbox_string.split(',')))

    # Create a dictionary with the required keys and values
    bbox = {
        'xmin': bbox_values[0],
        'xmax': bbox_values[2],
        'ymin': bbox_values[1],
        'ymax': bbox_values[3]
    }
    print("bbox")
    print(bbox)


    query = f"""
        SELECT
            ST_AsText(geometry) as geometry,
            ST_Y(ST_Centroid(ST_GeomFromText(ST_AsText(geometry)))) as latitude,
            ST_X(ST_Centroid(ST_GeomFromText(ST_AsText(geometry)))) as longitude
        FROM read_parquet('s3://overturemaps-us-west-2/release/2024-09-18.0/theme=buildings/type=building/*', filename=true, hive_partitioning=1)
        WHERE
            bbox.xmin <= {bbox["xmax"]}
            AND bbox.xmax >= {bbox["xmin"]}
            AND bbox.ymin <= {bbox["ymax"]}
            AND bbox.ymax >= {bbox["ymin"]}
        """
    print("query")
    print(query)   
    df_buildings = con.sql(query).df()
    print("df_buildings")

    df_buildings['geometry'] = df_buildings['geometry'].apply(shapely.wkt.loads)
    print("df_buildings sjoin")
    gdf_buildings = gpd.GeoDataFrame(df_buildings, geometry='geometry')
    gdf_buildings.set_crs(epsg=4326, inplace=True)

    gdf_joined = gdf_buildings.sjoin(gdf_cab)
    gdf_joined = gdf_joined.set_crs(epsg=4326)
    gdf_joined = gdf_joined.drop(columns=['index_right'])
    print("gdf_joined")
    print(gdf_joined)

    # Save the processed DataFrame
    gdf_joined.to_parquet(output_data)
    
    return gdf_joined

if __name__ == "__main__":
    df = create_travel_time_map()
    
    print("Converting geometries...")
    # Convert bytes to shapely geometry
    # df['geometry'] = df['geometry'].apply(shapely.wkb.loads)
    
    df.to_parquet("travel_time_data_centroids.parquet")
    
    print("Creating map...")
    # Create the map with dropdown
    fig = go.Figure()

    # Create a dropdown menu for days of the week
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    buttons = []
    
    # Add a trace for each day (initially visible=False)
    for day in days:
        day_data = df[df['day_of_week'] == day]
        fig.add_densitymapbox(
            lat=day_data['latitude'],
            lon=day_data['longitude'],
            radius=10,
            colorscale='Viridis',
            name=f'Building Density - {day}',
            visible=(day == 'Monday')  # Only Monday visible by default
        )
        
        # Create button for this day
        buttons.append(
            dict(
                label=day,
                method="update",
                args=[
                    {"visible": [d == day for d in days]},  # Show only selected day
                    {"title": f"NYC Taxi Trip Analysis - {day}"}  # Update title
                ]
            )
        )

    # Update the layout with dropdown menu
    fig.update_layout(
        updatemenus=[
            dict(
                buttons=buttons,
                direction="down",
                showactive=True,
                x=0.1,
                y=1.1,
                xanchor="left",
                yanchor="top"
            )
        ],
        mapbox=dict(
            style='carto-positron',
            center=dict(lat=40.7128, lon=-74.0060),
            zoom=11
        ),
        showlegend=True,
        title='NYC Taxi Trip Analysis - Monday',  # Default title
        height=800,
        margin=dict(t=100)  # Add margin at top for dropdown
    )

    fig.show()
    print("Saving map...")
    # Save to HTML
    fig.write_html(
        'nyc_taxi_map.html',
        include_plotlyjs=True,
        full_html=True
    )
    print("Map saved as 'nyc_taxi_map.html'")