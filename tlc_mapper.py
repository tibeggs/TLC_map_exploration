import pandas as pd
import duckdb
import geopandas as gpd 
import plotly.express as px
import plotly.graph_objects as go
import os
import json
import shapely
from keplergl import KeplerGl

def create_travel_time_map():
    """
    Create travel time map for all days of the week.
    Returns a DataFrame with data for all days.
    """
    # Generate unique filename
    input_data = "./data/yellow_tripdata_2010-05.parquet"
    local_buildings = "./data/db_nyc_buildings.parquet"
    output_data = f"db_travel_time_data_all_days.parquet"
    
    if os.path.exists(output_data):
        print(f"Loading existing data...")
        df = gpd.read_parquet(output_data)
        return df
    
    print(f"Generating new map data...")
    con = duckdb.connect("tlc_database_hour.duckdb")
    print("Installing extensions...")
    con.sql(""" INSTALL h3 FROM community;
                LOAD h3;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_region='us-west-2';""")

    # Modify the URL based on the date
    url = input_data
    min_cnt = 20
    resolution = 10
    
    # Create initial table from parquet data
    print("Creating initial table...")
    # Check if the table already exists
    table_exists = con.sql("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'taxi_data'").fetchone()[0] > 0

    if not table_exists:
        # con.sql(f"""
        #     CREATE OR REPLACE TABLE taxi_data AS
        #     SELECT DISTINCT
        #         h3_h3_to_string(h3_latlng_to_cell(pickup_latitude, pickup_longitude, {resolution})) AS cell_id, 
        #         h3_cell_to_boundary_wkt(cell_id) boundary,
        #         pickup_datetime
        #     FROM read_parquet('{url}')
        #     WHERE 
        #         -- Filter for second week (days 8-14) of the month
        #         EXTRACT(DAY FROM CAST(pickup_datetime AS TIMESTAMP)) BETWEEN 8 AND 14
        # """)
        con.sql(f"""
            CREATE OR REPLACE TABLE taxi_data AS
            SELECT DISTINCT
                h3_h3_to_string(h3_latlng_to_cell(pickup_latitude, pickup_longitude, {resolution})) AS cell_id, 
                h3_cell_to_boundary_wkt(cell_id) boundary,
                pickup_datetime
            FROM read_parquet('{url}')
        """)
    else:
        print("Table 'taxi_data' already exists.")

    # Create temporal analysis table
    print("Creating temporal analysis table...")
    table_exists = con.sql("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'temporal_data'").fetchone()[0] > 0

    if not table_exists:
        con.sql(f"""
            CREATE OR REPLACE TABLE temporal_data AS
            SELECT 
                cell_id,
                boundary,
                AVG(CASE WHEN hour_of_day = 0 THEN trip_count ELSE 0 END) as cnt_0,
                AVG(CASE WHEN hour_of_day = 1 THEN trip_count ELSE 0 END) as cnt_1,
                AVG(CASE WHEN hour_of_day = 2 THEN trip_count ELSE 0 END) as cnt_2,
                AVG(CASE WHEN hour_of_day = 3 THEN trip_count ELSE 0 END) as cnt_3,
                AVG(CASE WHEN hour_of_day = 4 THEN trip_count ELSE 0 END) as cnt_4,
                AVG(CASE WHEN hour_of_day = 5 THEN trip_count ELSE 0 END) as cnt_5,
                AVG(CASE WHEN hour_of_day = 6 THEN trip_count ELSE 0 END) as cnt_6,
                AVG(CASE WHEN hour_of_day = 7 THEN trip_count ELSE 0 END) as cnt_7,
                AVG(CASE WHEN hour_of_day = 8 THEN trip_count ELSE 0 END) as cnt_8,
                AVG(CASE WHEN hour_of_day = 9 THEN trip_count ELSE 0 END) as cnt_9,
                AVG(CASE WHEN hour_of_day = 10 THEN trip_count ELSE 0 END) as cnt_10,
                AVG(CASE WHEN hour_of_day = 11 THEN trip_count ELSE 0 END) as cnt_11,
                AVG(CASE WHEN hour_of_day = 12 THEN trip_count ELSE 0 END) as cnt_12,
                AVG(CASE WHEN hour_of_day = 13 THEN trip_count ELSE 0 END) as cnt_13,
                AVG(CASE WHEN hour_of_day = 14 THEN trip_count ELSE 0 END) as cnt_14,
                AVG(CASE WHEN hour_of_day = 15 THEN trip_count ELSE 0 END) as cnt_15,
                AVG(CASE WHEN hour_of_day = 16 THEN trip_count ELSE 0 END) as cnt_16,
                AVG(CASE WHEN hour_of_day = 17 THEN trip_count ELSE 0 END) as cnt_17,
                AVG(CASE WHEN hour_of_day = 18 THEN trip_count ELSE 0 END) as cnt_18,
                AVG(CASE WHEN hour_of_day = 19 THEN trip_count ELSE 0 END) as cnt_19,
                AVG(CASE WHEN hour_of_day = 20 THEN trip_count ELSE 0 END) as cnt_20,
                AVG(CASE WHEN hour_of_day = 21 THEN trip_count ELSE 0 END) as cnt_21,
                AVG(CASE WHEN hour_of_day = 22 THEN trip_count ELSE 0 END) as cnt_22,
                AVG(CASE WHEN hour_of_day = 23 THEN trip_count ELSE 0 END) as cnt_23
            FROM (
                SELECT 
                    cell_id,
                    boundary,
                    EXTRACT(HOUR FROM CAST(pickup_datetime AS TIMESTAMP)) as hour_of_day,
                    COUNT(*) as trip_count
                FROM taxi_data
                GROUP BY 1,2,3
                HAVING COUNT(*) >= {min_cnt}
            )
            GROUP BY 1,2
            ORDER BY cell_id
        """)
    else:
        print("Table 'temporal_data' already exists.")

    # Get results as DataFrame
    # df_temporal = con.sql("SELECT * FROM temporal_data").df()

    # gdf_cab=df_temporal
    # gdf_cab = gpd.GeoDataFrame(df_temporal.drop(columns=['boundary']), geometry=df_temporal.boundary.apply(shapely.wkt.loads))
    # gdf_cab = gdf_cab.set_crs(epsg=4326, inplace=True)
    # print('gdf_cab_1')
    # print(gdf_cab)

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

    # Create buildings table if loading from S3
    print("Creating buildings table...")
    table_exists = con.sql("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'buildings'").fetchone()[0] > 0

    if not table_exists:
        print("Downloading buildings data...")
        con.sql(f"""
            CREATE OR REPLACE TABLE buildings AS
            SELECT
                ST_AsText(geometry) as geometry
            FROM read_parquet('s3://overturemaps-us-west-2/release/2024-09-18.0/theme=buildings/type=building/*', filename=true, hive_partitioning=1)
            WHERE
                bbox.xmin <= {bbox["xmax"]}
                AND bbox.xmax >= {bbox["xmin"]}
                AND bbox.ymin <= {bbox["ymax"]}
                AND bbox.ymax >= {bbox["ymin"]}
        """)
        df_buildings = con.sql("SELECT * FROM buildings").df()
        df_buildings.to_parquet(local_buildings)
    else:
        print("Loading buildings from local file...")
        # df_buildings = pd.read_parquet(local_buildings)

    # Perform spatial join in DuckDB
    print("Performing spatial join in DuckDB...")
    table_exists = con.sql("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'gdf_joined'").fetchone()[0] > 0
    if not table_exists:
        con.sql("""
            CREATE OR REPLACE TABLE gdf_joined AS
            SELECT 
                b.geometry as geometry,
                c.cell_id,
                c.cnt_0,
                c.cnt_1,
                c.cnt_2,
                c.cnt_3,
                c.cnt_4,
                c.cnt_5,
                c.cnt_6,
                c.cnt_7,
                c.cnt_8,
                c.cnt_9,
                c.cnt_10,
                c.cnt_11,
                c.cnt_12,
                c.cnt_13,
                c.cnt_14,
                c.cnt_15,
                c.cnt_16,
                c.cnt_17,
                c.cnt_18,
                c.cnt_19,
                c.cnt_20,
                c.cnt_21,
                c.cnt_22,
                c.cnt_23
            FROM buildings AS b
            JOIN temporal_data AS c
            ON ST_Intersects(ST_GeomFromText(b.geometry), ST_GeomFromText(c.boundary))
        """)
    else:
        print("Loading gdf_joined from local file...")


    gdf_joined = con.sql("SELECT * FROM gdf_joined").df()
 
    print("gdf_joined")
    df_join = gpd.GeoDataFrame(gdf_joined, geometry=gdf_joined.geometry.apply(shapely.wkt.loads))
    # Preserve building geometry and add taxi data attributes
    df_join = df_join.set_crs(epsg=4326)
    # Keep building geometry and relevant taxi data
    df_join = df_join[['geometry', 'cell_id', 'cnt_0', 'cnt_1', 'cnt_2', 'cnt_3', 'cnt_4', 'cnt_5', 'cnt_6', 'cnt_7', 
                            'cnt_8', 'cnt_9', 'cnt_10', 'cnt_11', 'cnt_12', 'cnt_13', 'cnt_14', 'cnt_15', 'cnt_16', 
                            'cnt_17', 'cnt_18', 'cnt_19', 'cnt_20', 'cnt_21', 'cnt_22', 'cnt_23']]

    # Save the processed DataFrame
    # gdf_joined.to_parquet(output_data)
    
    return df_join

def create_kepler_map(gdf_joined):
    """
    Creates and returns a KeplerGl map configuration with the given GeoDataFrame
    """
    kepler_config = {
        'version': 'v1',
        'config': {
            'visState': {
                'filters': [],
                'layers': [
                    {
                        'id': f'buildings-layer-{hour}',
                        'type': 'geojson',
                        'config': {
                            'dataId': 'NYC Buildings with Taxi Data',
                            'label': f'Pickups - Hour {hour}',
                            'columns': {'geojson': 'geometry'},
                            'isVisible': hour == 0,  # Only hour 0 visible by default
                            'visConfig': {
                                'opacity': 0.8,
                                'filled': True,
                                'enable3d': False,
                                'colorRange': {
                                    'name': 'Ocean Blue',
                                    'type': 'sequential',
                                    'category': 'Uber',
                                    'colors': ['#008080', '#009999', '#00CCCC', '#00FFFF', '#FFCCCC', '#FF9999', '#FF6666', '#FF3333', '#FF0000']
                                }
                            }
                        },
                        'visualChannels': {
                            'colorField': {'name': f'cnt_{hour}', 'type': 'integer'},
                            'colorScale': 'quantile'
                        }
                    } for hour in range(24)
                ],
                'interactionConfig': {
                    'tooltip': {
                        'fieldsToShow': {
                            'NYC Buildings with Taxi Data': [
                                {'name': f'cnt_{hour}', 'format': None} for hour in range(24)
                            ]
                        },
                        'enabled': True
                    }
                }
            },
            'mapState': {
                'latitude': 40.7128,  # NYC center latitude
                'longitude': -74.0060,  # NYC center longitude
                'zoom': 11,
                'pitch': 0,
                'bearing': 0
            }
        }
    }
    
    # Create KeplerGL map
    map_1 = KeplerGl()
    map_1.add_data(data=gdf_joined, name='NYC Buildings with Taxi Data')
    map_1.config = kepler_config
    return map_1

if __name__ == "__main__":
    gdf_joined = create_travel_time_map()
    # gdf_joined = gdf_joined.sample(1000)
    map_1 = create_kepler_map(gdf_joined)
    map_1.save_to_html(file_name='db_big.html')