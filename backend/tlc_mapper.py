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
    local_buildings = "./data/nyc_buildings.parquet"
    output_data = f"travel_time_data_all_days.parquet"
    
    if os.path.exists(output_data):
        print(f"Loading existing data...")
        df = gpd.read_parquet(output_data)
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
    url = input_data
    min_cnt = 20
    resolution = 10
    
    query = f"""
        SELECT DISTINCT
            h3_h3_to_string(h3_latlng_to_cell(pickup_latitude, pickup_longitude, {resolution})) AS cell_id, 
            h3_cell_to_boundary_wkt(cell_id) boundary,
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
            FROM df
            GROUP BY 1,2,3
            HAVING COUNT(*) >= {min_cnt}
        )
        GROUP BY 1,2
        ORDER BY cell_id
    """
    print("temporal_query")
    df_temporal = con.sql(temporal_query).df()
    print("Temporal analysis results:")
    print(df_temporal.head())
    gdf_cab=df_temporal
    gdf_cab = gpd.GeoDataFrame(df_temporal.drop(columns=['boundary']), geometry=df_temporal.boundary.apply(shapely.wkt.loads))
    gdf_cab = gdf_cab.set_crs(epsg=4326, inplace=True)
    print('gdf_cab_1')
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

    # Modify the buildings query to use local file if it exists
    if os.path.exists(local_buildings):
        print("Loading buildings from local file...")
        df_buildings = pd.read_parquet(local_buildings)
    else:
        print("Downloading buildings data...")
        con.sql("INSTALL httpfs; LOAD httpfs; SET s3_region='us-west-2';")
        query = f"""
        SELECT
            ST_AsText(geometry) as geometry
        FROM read_parquet('s3://overturemaps-us-west-2/release/2024-09-18.0/theme=buildings/type=building/*', filename=true, hive_partitioning=1)
        WHERE
            bbox.xmin <= {bbox["xmax"]}
            AND bbox.xmax >= {bbox["xmin"]}
            AND bbox.ymin <= {bbox["ymax"]}
            AND bbox.ymax >= {bbox["ymin"]}
        
        """
        # Save the buildings data locally for future use
        df_buildings = con.sql(query).df()
        df_buildings.to_parquet(local_buildings)

    df_buildings['geometry'] = df_buildings['geometry'].apply(shapely.wkt.loads)
    print("df_buildings sjoin")
    gdf_buildings = gpd.GeoDataFrame(df_buildings, geometry='geometry')
    gdf_buildings.set_crs(epsg=4326, inplace=True)
    
    # Modify the spatial join to preserve building geometries
    gdf_joined = gpd.sjoin(gdf_buildings, gdf_cab, how='left')
    print("gdf_joined")
    # Preserve building geometry and add taxi data attributes
    gdf_joined = gdf_joined.set_crs(epsg=4326)
    # Keep building geometry and relevant taxi data
    gdf_joined = gdf_joined[['geometry', 'cell_id', 'cnt_monday', 'cnt_tuesday', 'cnt_wednesday', 
                            'cnt_thursday', 'cnt_friday', 'cnt_saturday', 'cnt_sunday']]

    # Save the processed DataFrame
    gdf_joined.to_parquet(output_data)
    
    return gdf_joined

if __name__ == "__main__":
    # df = create_travel_time_map()
    # df = df.to_wkt()
    # kepler_config = {'version': 'v1', 'config': {'visState': {'filters': [], 'layers': [{'id': 'jsbkpp9', 'type': 'geojson', 'config': {'dataId': 'Pickups - Buildings', 'label': 'Pickups - Buildings', 'color': [18, 147, 154], 'highlightColor': [252, 242, 26, 255], 'columns': {'geojson': 'geometry'}, 'isVisible': True, 'visConfig': {'opacity': 0.8, 'strokeOpacity': 0.8, 'thickness': 0.5, 'strokeColor': [221, 178, 124], 'colorRange': {'name': 'Global Warming', 'type': 'sequential', 'category': 'Uber', 'colors': ['#5A1846', '#900C3F', '#C70039', '#E3611C', '#F1920E', '#FFC300']}, 'strokeColorRange': {'name': 'Global Warming', 'type': 'sequential', 'category': 'Uber', 'colors': ['#5A1846', '#900C3F', '#C70039', '#E3611C', '#F1920E', '#FFC300']}, 'radius': 10, 'sizeRange': [0, 10], 'radiusRange': [0, 50], 'heightRange': [0, 500], 'elevationScale': 5, 'enableElevationZoomFactor': True, 'stroked': False, 'filled': True, 'enable3d': False, 'wireframe': False}, 'hidden': False, 'textLabel': [{'field': None, 'color': [255, 255, 255], 'size': 18, 'offset': [0, 0], 'anchor': 'start', 'alignment': 'center'}]}, 'visualChannels': {'colorField': {'name': 'cnt', 'type': 'integer'}, 'colorScale': 'quantile', 'strokeColorField': None, 'strokeColorScale': 'quantile', 'sizeField': None, 'sizeScale': 'linear', 'heightField': None, 'heightScale': 'linear', 'radiusField': None, 'radiusScale': 'linear'}}], 'interactionConfig': {'tooltip': {'fieldsToShow': {'Pickups - Buildings': [{'name': 'index_right', 'format': None}, {'name': 'cnt', 'format': None}]}, 'compareMode': False, 'compareType': 'absolute', 'enabled': True}, 'brush': {'size': 0.5, 'enabled': False}, 'geocoder': {'enabled': False}, 'coordinate': {'enabled': False}}, 'layerBlending': 'normal', 'splitMaps': [], 'animationConfig': {'currentTime': None, 'speed': 1}}, 'mapState': {'bearing': 0, 'dragRotate': False, 'latitude': 40.74413534782696, 'longitude': -73.94421276319235, 'pitch': 0, 'zoom': 10.565938787607614, 'isSplit': False}, 'mapStyle': {'styleType': 'dark', 'topLayerGroups': {}, 'visibleLayerGroups': {'label': False, 'road': False, 'border': False, 'building': True, 'water': True, 'land': True, '3d building': False}, 'threeDBuildingColor': [9.665468314072013, 17.18305478057247, 31.1442867897876], 'mapStyles': {}}}}
    gdf_joined=create_travel_time_map()
    gdf_joined=gdf_joined.sample(10000)
    # df = df.to_wkt()
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
                            'dataId': 'NYC Buildings with Taxi Data',
                            'label': f'Pickups - {day.capitalize()}',
                            'columns': {'geojson': 'geometry'},
                            'isVisible': day == 'monday',  # Only Monday visible by default
                            'visConfig': {
                                'opacity': 0.8,
                                'filled': True,
                                'enable3d': False,
                                'colorRange': {
                                    'name': 'Global Warming',
                                    'type': 'sequential',
                                    'category': 'Uber',
                                    'colors': ['#5A1846', '#900C3F', '#C70039', '#E3611C', '#F1920E', '#FFC300']
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
    map_1.add_data(data=gdf_joined, name='NYC Buildings with Taxi Data')
    map_1.config = kepler_config
    # Save the map
    map_1.save_to_html(file_name='nyc_buildings_taxi_kepler_mapper_dow.html')