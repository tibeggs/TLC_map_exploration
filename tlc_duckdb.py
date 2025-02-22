
local_buildings = "./data/db_nyc_buildings.parquet"
def install_extensions(con):
    print("Installing extensions...")
    con.sql(""" INSTALL h3 FROM community;
                LOAD h3;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_region='us-west-2';""")
    
def create_taxi_table(con, resolution, url):
    table_exists = con.sql("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'taxi_data'").fetchone()[0] > 0
    if not table_exists:
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

def create_buildings_table(con):
    print("Checking Building table status")
    table_exists = con.sql("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'buildings'").fetchone()[0] > 0
    # Create buildings table if loading from S3
    print("Creating buildings table...")
    
    if not table_exists:
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
