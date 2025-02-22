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
    df_table_name_postfix = "day"
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
                MAX(CASE WHEN day_of_week = 'Monday' THEN trip_count ELSE 0 END) as cnt_monday,
                MAX(CASE WHEN day_of_week = 'Tuesday' THEN trip_count ELSE 0 END) as cnt_tuesday,
                MAX(CASE WHEN day_of_week = 'Wednesday' THEN trip_count ELSE 0 END) as cnt_wednesday,
                MAX(CASE WHEN day_of_week = 'Thursday' THEN trip_count ELSE 0 END) as cnt_thursday,
                MAX(CASE WHEN day_of_week = 'Friday' THEN trip_count ELSE 0 END) as cnt_friday,
                MAX(CASE WHEN day_of_week = 'Saturday' THEN trip_count ELSE 0 END) as cnt_saturday,
                MAX(CASE WHEN day_of_week = 'Sunday' THEN trip_count ELSE 0 END) as cnt_sunday
            FROM (
                SELECT 
                    cell_id,
                    boundary,
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
                FROM taxi_data
                GROUP BY 1,2,3
                HAVING COUNT(*) >= {min_cnt}
            )
            GROUP BY 1,2
            ORDER BY cell_id
        """)
    else:
        print("Table 'taxi_data' already exists.")

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
                c.cnt_monday,
                c.cnt_tuesday,
                c.cnt_wednesday,
                c.cnt_thursday,
                c.cnt_friday,
                c.cnt_saturday,
                c.cnt_sunday
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
    df_join = df_join[['geometry', 'cell_id', 'cnt_monday', 'cnt_tuesday', 'cnt_wednesday', 
                            'cnt_thursday', 'cnt_friday', 'cnt_saturday', 'cnt_sunday']]
    
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
                        'id': f'buildings-layer-{day}',
                        'type': 'geojson',
                        'config': {
                            'dataId': 'NYC Taxi Pickup by Day',
                            'label': f'Pickups - {day.capitalize()}',
                            'columns': {'geojson': 'geometry'},
                            'isVisible': day == 'monday',  # Only Monday visible by default
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
                            'colorField': {'name': f'cnt_{day}', 'type': 'integer'},
                            'colorScale': 'quantile'
                        }
                    } for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                ],
                'interactionConfig': {
                    'tooltip': {
                        'fieldsToShow': {
                            'NYC Buildings with Taxi Data': [
                                {'name': 'cnt_monday', 'format': None},
                                {'name': 'cnt_tuesday', 'format': None},
                                {'name': 'cnt_wednesday', 'format': None},
                                {'name': 'cnt_thursday', 'format': None},
                                {'name': 'cnt_friday', 'format': None},
                                {'name': 'cnt_saturday', 'format': None},
                                {'name': 'cnt_sunday', 'format': None}
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
    map_1.add_data(data=gdf_joined, name='NYC Taxi Pickup by Day')
    map_1.config = kepler_config
    map_1.save_to_html(file_name='tlc_kgl_daily.html')
