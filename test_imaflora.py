# -*- coding: utf-8 -*-
"""
Created on Sat Feb 22 11:18:03 2025

@author: Gabriel
"""

from owslib.fes import *
from owslib.etree import etree
from owslib.wfs import WebFeatureService
from psycopg2 import sql
import psycopg2
from psycopg2.extras import execute_values
import geopandas as gpd
import pandas as pd
from shapely.validation import make_valid
from dotenv import find_dotenv, load_dotenv
import os 
import json
import sys
import logging
import time
from sqlalchemy import create_engine
import datetime

# os.chdir(r'C:\Projetos\teste_imaflora')

script_path = os.path.dirname(os.path.abspath(__file__))

os.chdir(script_path)

class EtlTerraBrasilis:
    def __init__(self, workspace, layer_name, start, end):
        self.start = start
        self.end = end
        self.workspace = workspace
        self.layer_name = layer_name
        
    
    def checkWsAndLayer(self ,json_data):
        
        ws = self.workspace
        layer = self.layer_name
        
        # Check if the layer exists in the JSON data
        if ws not in json_data:
            return {
                "ws_exists": False,
                "layer_exists": False,
                "has_year_column": False,
                "message": f"Workspace '{ws}' not found."
            }
        
        # Check if the content exists in the layer
        if layer not in json_data[ws]["layers"]:
            return {
                "ws_exists": True,
                "layer_exists": False,
                "has_year_column": False,
                "message": f"Content '{layer}' not found in workspace '{ws}'."
            }
        
        # Get the schema for the content
        schema = json_data[ws]["layers"][layer].get("schema")
        
        # Check if the schema exists and contains a "year" column
        if schema and "properties" in schema and "year" in schema["properties"]:
            return {
                "ws_exists": True,
                "layer_exists": True,
                "has_year_column": True,
                "message": f"Layer '{layer}' in workspace '{ws}' has a 'year' column."
            }
        else:
            return {
                "ws_exists": True,
                "layer_exists": True,
                "has_year_column": False,
                "message": f"Layer '{layer}' in workspace '{ws}' does not have a 'year' column."
            }

    def callWfs(self, result, wfs_url):

        wfs = WebFeatureService(url=wfs_url, version="1.1.0")        
        
        max_attempts = 100
        attempt = 0
        
        while attempt < max_attempts:
            try:
                if result['has_year_column']:
                    filter_start = PropertyIsGreaterThanOrEqualTo(propertyname='year', literal=self.start)
                    filter_end = PropertyIsLessThanOrEqualTo(propertyname='year', literal=self.end)
                    
                    combined_filter = And([filter_start, filter_end])
                    
                    filterxml = etree.tostring(combined_filter.toXML()).decode("utf-8")
                    response = wfs.getfeature(typename=self.layer_name, filter=filterxml, outputFormat='application/json')
                else:
                    response = wfs.getfeature(typename=layer_name, outputFormat='application/json')
                return response
            except Exception as e:
                attempt += 1
                logging.error(f"Attempt {attempt} failed: {e}")
                time.sleep(10)
        
        raise Exception("Failed to connect after 100 attempts")
        
    def treatGeometry(self, response):  
        try:
            gdf = gpd.read_file(response)
            
            # Step 1: Check and fix invalid geometries
            logging.info("Checking for invalid geometries")
            invalid = ~gdf.geometry.is_valid
            
            valid_count = gdf.geometry.is_valid.sum()
            logging.info(f"Valid geometries count: {valid_count}")
            
            if invalid.any():
                logging.info(f"Found {invalid.sum()} invalid geometries. Fixing them")
                gdf.loc[invalid, 'geometry'] = gdf.loc[invalid, 'geometry'].apply(make_valid)
            
            # Step 2: Reproject to EPSG:3857
            logging.info("Reprojecting to EPSG:3857")
            gdf = gdf.to_crs(epsg=3857)
            
            return(gdf)
        
        except Exception as e:
            logging.error(f'treatGeometry: {e}')
            
    def mapDtypeToPg(self,pandas_dtype):
        if pd.api.types.is_integer_dtype(pandas_dtype):
            return 'INTEGER'
        elif pd.api.types.is_float_dtype(pandas_dtype):
            return 'FLOAT'
        elif pd.api.types.is_bool_dtype(pandas_dtype):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
            return 'TIMESTAMP'
        elif pd.api.types.is_string_dtype(pandas_dtype):
            return 'TEXT'
        else:
            return 'TEXT'  # Default to TEXT for unknown types
        
    def configPostgres(self, cur, table_name, gdf):
        try:
            schema_name = 'raw_data'
            
            # Check if schema exists
            cur.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';")
            schema_exists = cur.fetchone()
            
            if not schema_exists:
                # Create schema
                cur.execute(sql.SQL("CREATE SCHEMA {schema_name};"))
                logging.info("Schema '{schema_name}' created.")
            
            # Check if PostGIS extension is installed
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_extension 
                    WHERE extname = 'postgis'
                );
            """)
            postgis_installed = cur.fetchone()[0]
            
            if not postgis_installed:
                # Install PostGIS extension
                cur.execute("CREATE EXTENSION postgis;")
                logging.info("PostGIS extension installed.")
            
            # Check if table exists
            cur.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{schema_name}' 
                    AND table_name = '{table_name}'
                );
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                columns = []
                for col in gdf.columns:
                    if col == 'geometry':
                        # Handle geometry column separately
                        columns.append(f"{col} GEOMETRY(Geometry, 3857)")
                    else:
                        pg_dtype = self.mapDtypeToPg(gdf[col].dtype)
                        columns.append(f"{col} {pg_dtype}")
                
                # Construct the CREATE TABLE query
                create_table_query = sql.SQL("""
                    CREATE TABLE {schema}.{table} (
                        {columns}
                    );
                """).format(
                    schema=sql.Identifier(schema_name),
                    table=sql.Identifier(table_name),
                    columns=sql.SQL(',\n        ').join(map(sql.SQL, columns))
                )
                
                # Execute the query
                try:
                    cur.execute(create_table_query)
                    logging.info(f"Table '{schema_name}.{table_name}' created successfully.")
                    cur.execute(f"""
                        CREATE INDEX idx_geometry_{table_name} ON {schema_name}.{table_name} USING GIST (geometry);
                    """)
                    logging.info("Indexes created on the table.")
                except Exception as e:
                    logging.error(f"Error creating table: {e}")
            
        except Exception as e:
            logging.error(f'configPostgres: {e}')
    
    def insertData(self, gdf, engine, table_name):
        
        schema_name = 'raw_data'
        try:
            
            # Write the GeoDataFrame to the PostGIS table
            gdf.to_postgis(
                name=table_name,  # Table name
                con=engine,       # SQLAlchemy engine
                schema=schema_name,  # Schema name
                if_exists='append',  # Options: 'fail', 'replace', 'append'
                index=False,      # Do not write the index column
            )
                
        except Exception as e:
            logging.error(f'insertData: {e}')
    
    
        

if __name__ == '__main__':
    
    now = datetime.datetime.now().strftime("%I.%M%p_%B_%d_%Y")
    logging.basicConfig(
        filename=f"etl_terrabrasilis_{now}.log",
        filemode='a',
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )
    
    t = EtlTerraBrasilis(sys.argv[1], sys.argv[2],
                              sys.argv[3], sys.argv[4])
    
    # t = EtlTerraBrasilis("prodes-cerrado-nb", "yearly_deforestation",
    #                           "2023", "2025")
    
    logging.info('Initializing process...')
    
    with open("wfs_info.json", "r") as json_file:
        wfs_json_data = json.load(json_file)
    
    result = t.checkWsAndLayer(wfs_json_data)
    
    if not result['layer_exists']:
        logging.error('Layer does not exist. Exiting script.')
        sys.exit('Layer does not exist. Exiting script.')
    else:
        wfs_url = f"https://terrabrasilis.dpi.inpe.br/geoserver/{sys.argv[1]}/{sys.argv[2]}/wfs"
        # wfs_url = "https://terrabrasilis.dpi.inpe.br/geoserver/prodes-cerrado-nb/yearly_deforestation/wfs"
    
    response = t.callWfs(result, wfs_url)
    geometry = t.treatGeometry(response)
    
    load_dotenv(find_dotenv('config.env'))
        
    db_params = {
        'dbname': os.getenv('DATABASE'),
        'user':  os.getenv('USER'),
        'password': os.getenv('PASSWORD'),
        'host': os.getenv('HOST'),
        'port': os.getenv('PORT')
    }
    
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    db_url = f"postgresql+psycopg2://{os.getenv('USER')}:{os.getenv('PASSWORD')}@{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('DATABASE')}"

    # Create a SQLAlchemy engine
    engine = create_engine(db_url)
    
    table_name = f"{sys.argv[1]}_{sys.argv[2]}".replace("-", "_")
    # table_name = "prodes-cerrado-nb_yearly_deforestation".replace("-", "_")
    
    t.configPostgres(cur, table_name, geometry)
    conn.commit()
    
    t.insertData(geometry, engine, table_name)
    
    # Close the cursor and connection
    cur.close()
    conn.close()
    logging.info("Process Concluded.")

