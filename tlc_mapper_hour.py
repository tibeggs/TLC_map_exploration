import pandas as pd
import duckdb
import geopandas as gpd 
import plotly.express as px
import plotly.graph_objects as go
import os
import shapely
from keplergl import KeplerGl
from tlc_duckdb import install_extensions, create_taxi_table, create_buildings_table

def create_travel_time_map():
    """
    Create travel time map for all days of the week.
    Returns a DataFrame with data for all days.
    """
    # Generate unique filename
    input_data = "./data/yellow_tripdata_2010-05.parquet"
    
    output_data = f"db_travel_time_data_all_days.parquet"
    duck_db_path = "tlc_database.duckdb"
    df_table_name_postfix = "hour"
    temporal_table_name = f"temporal_data_{df_table_name_postfix}"
    gdf_joined_table_name = f"gdf_joined_{df_table_name_postfix}"
    
    if os.path.exists(output_data):
        print(f"Loading existing data...")
        df = gpd.read_parquet(output_data)
        return df
    
    print(f"Generating map data...")
    con = duckdb.connect(duck_db_path)
    # Modify the URL based on the date
    url = input_data
    min_cnt = 20
    resolution = 10
    install_extensions(con)
    
    # Create initial table from parquet data
    print("Creating initial table...")
    # Check if the table already exists
    create_taxi_table(con, resolution, url)

    # Create temporal analysis table
    print("Creating temporal analysis table...")
    table_exists = con.sql(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{temporal_table_name}'").fetchone()[0] > 0

    if not table_exists:
        con.sql(f"""
            CREATE OR REPLACE TABLE {temporal_table_name} AS
            SELECT 
                cell_id,
                boundary,
                MAX(CASE WHEN hour_of_day = 0 THEN trip_count ELSE 0 END) as cnt_0,
                MAX(CASE WHEN hour_of_day = 1 THEN trip_count ELSE 0 END) as cnt_1,
                MAX(CASE WHEN hour_of_day = 2 THEN trip_count ELSE 0 END) as cnt_2,
                MAX(CASE WHEN hour_of_day = 3 THEN trip_count ELSE 0 END) as cnt_3,
                MAX(CASE WHEN hour_of_day = 4 THEN trip_count ELSE 0 END) as cnt_4,
                MAX(CASE WHEN hour_of_day = 5 THEN trip_count ELSE 0 END) as cnt_5,
                MAX(CASE WHEN hour_of_day = 6 THEN trip_count ELSE 0 END) as cnt_6,
                MAX(CASE WHEN hour_of_day = 7 THEN trip_count ELSE 0 END) as cnt_7,
                MAX(CASE WHEN hour_of_day = 8 THEN trip_count ELSE 0 END) as cnt_8,
                MAX(CASE WHEN hour_of_day = 9 THEN trip_count ELSE 0 END) as cnt_9,
                MAX(CASE WHEN hour_of_day = 10 THEN trip_count ELSE 0 END) as cnt_10,
                MAX(CASE WHEN hour_of_day = 11 THEN trip_count ELSE 0 END) as cnt_11,
                MAX(CASE WHEN hour_of_day = 12 THEN trip_count ELSE 0 END) as cnt_12,
                MAX(CASE WHEN hour_of_day = 13 THEN trip_count ELSE 0 END) as cnt_13,
                MAX(CASE WHEN hour_of_day = 14 THEN trip_count ELSE 0 END) as cnt_14,
                MAX(CASE WHEN hour_of_day = 15 THEN trip_count ELSE 0 END) as cnt_15,
                MAX(CASE WHEN hour_of_day = 16 THEN trip_count ELSE 0 END) as cnt_16,
                MAX(CASE WHEN hour_of_day = 17 THEN trip_count ELSE 0 END) as cnt_17,
                MAX(CASE WHEN hour_of_day = 18 THEN trip_count ELSE 0 END) as cnt_18,
                MAX(CASE WHEN hour_of_day = 19 THEN trip_count ELSE 0 END) as cnt_19,
                MAX(CASE WHEN hour_of_day = 20 THEN trip_count ELSE 0 END) as cnt_20,
                MAX(CASE WHEN hour_of_day = 21 THEN trip_count ELSE 0 END) as cnt_21,
                MAX(CASE WHEN hour_of_day = 22 THEN trip_count ELSE 0 END) as cnt_22,
                MAX(CASE WHEN hour_of_day = 23 THEN trip_count ELSE 0 END) as cnt_23
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

    create_buildings_table(con)

    # Perform spatial join in DuckDB
    print("Performing spatial join in DuckDB...")
    table_exists = con.sql(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{gdf_joined_table_name}'").fetchone()[0] > 0
    if not table_exists:
        con.sql(f"""
            CREATE OR REPLACE TABLE {gdf_joined_table_name} AS
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
            JOIN {temporal_table_name} AS c
            ON ST_Intersects(ST_GeomFromText(b.geometry), ST_GeomFromText(c.boundary))
        """)
    else:
        print("Loading gdf_joined from local file...")


    gdf_joined = con.sql(f"SELECT * FROM {gdf_joined_table_name}").df()
 
    print("gdf_joined")
    df_join = gpd.GeoDataFrame(gdf_joined, geometry=gdf_joined.geometry.apply(shapely.wkt.loads))
    # Preserve building geometry and add taxi data attributes
    df_join = df_join.set_crs(epsg=4326)
    # Keep building geometry and relevant taxi data
    df_join = df_join[['geometry', 'cell_id', 'cnt_0', 'cnt_1', 'cnt_2', 'cnt_3', 'cnt_4', 'cnt_5', 'cnt_6', 'cnt_7', 
                            'cnt_8', 'cnt_9', 'cnt_10', 'cnt_11', 'cnt_12', 'cnt_13', 'cnt_14', 'cnt_15', 'cnt_16', 
                            'cnt_17', 'cnt_18', 'cnt_19', 'cnt_20', 'cnt_21', 'cnt_22', 'cnt_23']]
    
    return df_join


if __name__ == "__main__":
    gdf_joined = create_travel_time_map()
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
    map_1.save_to_html(file_name='tlc_kgl_hourly.html')
