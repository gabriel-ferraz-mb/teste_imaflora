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
from shapely.validation import make_valid
from dotenv import find_dotenv, load_dotenv
import os 
import sys
import logging

script_path = os.path.dirname(os.path.abspath(__file__))

os.chdir(script_path)

class DownloadEmbargoIbama:
    def __init__(self, start, end, uf):
        self.start = start
        self.end = end
        self.uf = uf

        wfs_url = "https://geoservicos.inde.gov.br/geoserver/ICMBio/wfs"
        
        self.wfs = WebFeatureService(url=wfs_url, version="1.1.0")
        
        self.layer_name = 'ICMBio:embargos_icmbio'
    
    def callWfs(self):
        
        filter1 = PropertyIsLike(propertyname='uf', literal=self.uf, matchCase=False)
        filter2 = PropertyIsGreaterThanOrEqualTo(propertyname='ano', literal=self.start)
        filter3 = PropertyIsLessThanOrEqualTo(propertyname='ano', literal=self.end)
        
        combined_filter = And([filter1, filter2, filter3])
        
        filterxml = etree.tostring(combined_filter.toXML()).decode("utf-8")
        
        max_attempts = 100
        attempt = 0
        
        while attempt < max_attempts:
            try:
                response = self.wfs.getfeature(typename=self.layer_name, filter=filterxml, outputFormat='application/json')
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
            if invalid.any():
                logging.info(f"Found {invalid.sum()} invalid geometries. Fixing them")
                gdf.loc[invalid, 'geometry'] = gdf.loc[invalid, 'geometry'].apply(make_valid)
            
            # Step 2: Reproject to EPSG:3857
            logging.info("Reprojecting to EPSG:3857")
            gdf = gdf.to_crs(epsg=3857)
            
            # Step 3: Keep only the required columns
            required_columns = ['ogc_fid', 'cpf_cnpj', 'uf', 'data', 'geometry']
            gdf = gdf[required_columns]
            return(gdf)
        
        except Exception as e:
            logging.error(f'treatGeometry: {e}')

    def insertData(self, gdf, cur):
        
        try:
            # Convert the GeoDataFrame to a list of tuples
            data_tuples = list(zip(
                gdf['ogc_fid'],
                gdf['cpf_cnpj'],
                gdf['uf'],
                gdf['data'],
                gdf['geometry'].apply(lambda geom: geom.wkt)  # Convert geometry to WKT
            ))
    
            # SQL query to check for duplicates and insert new records
            insert_query = """
                INSERT INTO raw_data.embargos_ibama (gml_id, cpf_cnpj, uf, date, geometry)
                SELECT %s, %s, %s, %s, ST_GeomFromText(%s, 3857)
                WHERE NOT EXISTS (
                    SELECT 1 FROM raw_data.embargos_ibama WHERE gml_id = %s
                );
            """
    
            # Execute the query for each row
            for row in data_tuples:
                cur.execute(insert_query, row + (row[0],))
            
            logging.info("Rows inserted successfully.")
        
        except Exception as e:
            logging.error(f'insertData: {e}')
    
    def configPostgres(self, cur):
        try:
            # Check if schema exists
            cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'raw_data';")
            schema_exists = cur.fetchone()
            
            if not schema_exists:
                # Create schema
                cur.execute(sql.SQL("CREATE SCHEMA raw_data;"))
                logging.info("Schema 'raw_data' created.")
            
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
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'raw_data' 
                    AND table_name = 'embargos_ibama'
                );
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                # Create table
                cur.execute("""
                    CREATE TABLE raw_data.embargos_ibama (
                        gml_id SERIAL PRIMARY KEY,
                        cpf_cnpj TEXT,
                        uf TEXT,
                        date DATE,
                        geometry GEOMETRY(Geometry, 3857)
                    );
                """)
                logging.info("Table 'embargos_ibama' created in schema 'raw_data'.")
            
             # Create indexes
                cur.execute("""
                    CREATE INDEX idx_embargos_ibama_cpf_cnpj ON raw_data.embargos_ibama (cpf_cnpj);
                    CREATE INDEX idx_embargos_ibama_geometry ON raw_data.embargos_ibama USING GIST (geometry);
                """)
                logging.info("Indexes created on the table.")
                
        except Exception as e:
            logging.error(f'configPostgres: {e}')
    
    def execute(self):
        
        logging.info('Initializing process...')
        response = self.callWfs()
        geometry = self.treatGeometry(response)
        
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
        
        self.configPostgres(cur)
        conn.commit()
        
        self.insertData(geometry, cur)
        conn.commit()
        
        # Close the cursor and connection
        cur.close()
        conn.close()
        logging.info("Process Concluded.")

if __name__ == '__main__':
    
    logging.basicConfig(
        filename="embargo_ibama.log",
        filemode='a',
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )
    
    t = DownloadEmbargoIbama(sys.argv[1], sys.argv[2], sys.argv[3])
    t.execute()








