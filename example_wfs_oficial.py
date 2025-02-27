# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 22:32:46 2025

@author: Gabriel
"""

import requests, os, io
from datetime import datetime
from xml.etree import ElementTree as xmlTree
import logging
import psycopg2
from psycopg2 import sql
import fiona
import pandas as pd
from dotenv import find_dotenv, load_dotenv
import json
from shapely.geometry import shape, mapping
from shapely.validation import make_valid
from psycopg2.extras import execute_values
import sys
import shutil
import time

# os.chdir(r'C:\Projetos\teste_imaflora')
script_path = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_path)

class DownloadWFS:

  def __init__(self, logger: logging.Logger, biome: str, sd: str = "2000-01-01", ed: str = "2025-01-01"):
   
    self.START_DATE=sd
    self.END_DATE=ed

    self.WORKSPACE_NAME=f"prodes-{biome}-nb"
    
    if biome == "amazon":
        self.LAYER_NAME="yearly_deforestation_biome"
    else:
        self.LAYER_NAME="yearly_deforestation"

    # The output file name (layer_name_start_date_end_date)
    self.OUTPUT_FILENAME="{0}_{1}_{2}".format(self.LAYER_NAME,self.START_DATE,self.END_DATE)
    self.logger = logger
    self.schema_name = 'raw_data'
    self.table_name = f"{self.WORKSPACE_NAME}_{self.LAYER_NAME}".replace("-","_")

  def __buildBaseURL(self):
    host="terrabrasilis.dpi.inpe.br"
    url="https://{0}/geoserver/{1}/{2}/wfs".format(host,self.WORKSPACE_NAME,self.LAYER_NAME)
    return url

  def __buildQueryString(self, OUTPUTFORMAT=None):
        
    allLocalParams = {'OUTPUTFORMAT': ("application/json" if not OUTPUTFORMAT else OUTPUTFORMAT),
              'CQL_FILTER': "image_date BETWEEN '{0}' AND '{1}'".format(self.START_DATE,self.END_DATE),
              'SERVICE': 'WFS',
              'REQUEST': 'GetFeature',
              'VERSION': '2.0.0',
              'exceptions': 'text/xml',
              'srsName': 'EPSG:4674', 
              'TYPENAME': "{0}:{1}".format(self.WORKSPACE_NAME,self.LAYER_NAME)}    
    
    PARAMS="&".join("{}={}".format(k,v) for k,v in allLocalParams.items())
    
    return PARAMS

  def __xmlRequest(self, url):
    root=None
    response=requests.get(url)
    
    if response.ok:
      xmlInMemory = io.BytesIO(response.content)
      tree = xmlTree.parse(xmlInMemory)
      root = tree.getroot()
         
    return root

  def __getServerLimit(self):
    """
    Read the data download service limit via WFS
    """
    url="https://terrabrasilis.dpi.inpe.br/geoserver/ows?service=wfs&version=2.0.0&request=GetCapabilities"
    # the default limit on our GeoServer
    serverLimit=100000

    XML=self.__xmlRequest(url)

    if XML.tag is not None and '{http://www.opengis.net/wfs/2.0}WFS_Capabilities'==XML.tag:
      for p in XML.findall(".//{http://www.opengis.net/ows/1.1}Operation/[@name='GetFeature']"):
        dv=p.find(".//{http://www.opengis.net/ows/1.1}Constraint/[@name='CountDefault']")
        serverLimit=dv.find('.//{http://www.opengis.net/ows/1.1}DefaultValue').text
    else:
        self.logger.error("Failed to get Server Limit")

    return int(serverLimit)

  def __countMaxResult(self):
    """
    Read the number of lines of results expected in the download using the defined filters.
    """
    workspace = self.WORKSPACE_NAME
    layer = self.LAYER_NAME
    
    url=f"https://terrabrasilis.dpi.inpe.br/geoserver/wfs?SERVICE=WFS&REQUEST=GetFeature&VERSION=1.1.0&TYPENAME={workspace}:{layer}&resulttype=hits"
    numberMatched=0

    XML=self.__xmlRequest(url)
    if XML.tag is not None and '{http://www.opengis.net/wfs}FeatureCollection'==XML.tag:
      numberMatched=XML.find('[@numberOfFeatures]').get('numberOfFeatures')
    else:
        self.logger.error("Failed to get Max Count")
    return int(numberMatched)

  def pagination(self):
    # get server limit and count max number of results
    sl=self.__getServerLimit()
    rr=self.__countMaxResult()
    # define the start page number
    pagNumber=1
    # define the start index of data
    startIndex=0
    # define the attribute to sort data
    sortBy="uid"
    # using the server limit to each download
    count=sl
    # pagination iteraction
    while(startIndex<rr):
      paginationParams="&count={0}&sortBy={1}&startIndex={2}".format(count,sortBy,startIndex)
      self.__download(paginationParams,pagNumber)
      startIndex=startIndex+count
      pagNumber=pagNumber+1
    self.logger.info("Download done!")

  def __download(self, pagination="startIndex=0", pagNumber=1):
    base_url = self.__buildBaseURL()
    query = self.__buildQueryString()
    
    url="{0}?{1}&{2}".format(base_url, query, pagination)
       
    if not os.path.exists("results"):
        os.makedirs("results")
    
    # the extension of output file is ".zip" because the OUTPUTFORMAT is defined as "SHAPE-ZIP"
    output_file="results/{0}_part{1}.geojson".format(self.OUTPUT_FILENAME, pagNumber)
    
    max_retries = 100
    retry_count = 0
    retry_delay = 5  # Delay in seconds between retries

    while retry_count < max_retries:
        try:
            response = requests.get(url)
            
            if response.ok:
                with open(output_file, 'wb') as f:
                    f.write(response.content)
                break  # Exit the loop if the download is successful
            else:
                self.logger.error("Download failed with HTTP Error: {0}. Retrying... (Attempt {1}/{2})".format(
                    response.status_code, retry_count + 1, max_retries))
        
        except requests.exceptions.RequestException as e:
            self.logger.error("Download failed with exception: {0}. Retrying... (Attempt {1}/{2})".format(
                e, retry_count + 1, max_retries))
        
        retry_count += 1
        time.sleep(retry_delay)  # Add a delay before the next retry

    if retry_count == max_retries:
        self.logger.error("Download failed after {0} attempts.".format(max_retries))
      
  def __mapDtypeToPg(self, pandas_dtype) -> str:
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

  def configPostgres(self, conn, geojson_path) -> None:
        try:
            cur = conn.cursor()
            
            schema_name = self.schema_name
            table_name = self.table_name
            
            # Check if schema exists
            cur.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';")
            schema_exists = cur.fetchone()
            
            if not schema_exists:
                cur.execute(sql.SQL(f"CREATE SCHEMA {schema_name};"))
                self.logger.info(f"Schema '{schema_name}' created.")
            
            # Check if PostGIS extension is installed
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_extension 
                    WHERE extname = 'postgis'
                );
            """)
            postgis_installed = cur.fetchone()[0]
            
            if not postgis_installed:
                cur.execute("CREATE EXTENSION postgis;")
                self.logger.info("PostGIS extension installed.")
                conn.commit()
            
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
                # Read the GeoJSON file using fiona to get the schema
                with fiona.open(geojson_path) as src:
                    # Extract column names and data types
                    columns = []
                    for col_name, col_type in src.schema['properties'].items():
                        pg_dtype = self.__mapDtypeToPg(col_type)
                        columns.append(f"{col_name} {pg_dtype}")
                    
                    # Add geometry column
                    columns.append("geometry GEOMETRY(Geometry, 4674)")
                
                # Create the table
                create_table_query = sql.SQL("""
                    CREATE TABLE {schema}.{table} (
                        {columns}
                    );
                """).format(
                    schema=sql.Identifier(schema_name),
                    table=sql.Identifier(table_name),
                    columns=sql.SQL(',\n        ').join(map(sql.SQL, columns))
                )
                
                try:
                    cur.execute(create_table_query)
                    self.logger.info(f"Table '{schema_name}.{table_name}' created successfully.")
                    conn.commit()
                    # Create spatial index on the geometry column
                    cur.execute(f"""
                        CREATE INDEX idx_geometry_{table_name} ON {schema_name}.{table_name} USING GIST (geometry);
                    """)
                    conn.commit()
                    self.logger.info("Indexes created on the table.")
                    
                    cur.execute(f"""
                            DO $$
                            BEGIN
                                IF NOT EXISTS (
                                    SELECT 1
                                    FROM pg_constraint
                                    WHERE conrelid = '{schema_name}.{table_name}'::regclass
                                    AND conname = '{schema_name}.{table_name}'
                                ) THEN
                                    ALTER TABLE {schema_name}.{table_name}
                                    ADD CONSTRAINT id_unique_{table_name} UNIQUE (id);
                                END IF;
                            END $$;
                            """)
                    conn.commit()
                    self.logger.info("ID constraint created on table.")
                except Exception as e:
                    self.logger.error(f"Error creating table: {e}")
            cur.close()
        except Exception as e:
            self.logger.error(f'configPostgres: {e}')

  def __treatGeometry(self, geojson_paths):
        valid_count = 0
        invalid_count = 0
        processed_features = []
        
        for path in geojson_paths:
            path = os.path.join('results', path)
            with open(path, 'r', encoding='utf-8') as f:
                geojson_data = json.load(f)
                for feature in geojson_data['features']:
                    geometry = shape(feature['geometry'])
        
                    # Check if the geometry is valid
                    if not geometry.is_valid:
                        invalid_count += 1
                        geometry = make_valid(geometry)  # Fix invalid geometry
                    else:
                        valid_count += 1
        
                    # Update the feature with the (possibly fixed) geometry
                    feature['geometry'] = mapping(geometry)  # Convert back to GeoJSON format
                    processed_features.append(feature)
        
        # Log the counts of valid and invalid geometries
        self.logger.info(f"Valid geometries: {valid_count}")
        self.logger.info(f"Invalid geometries: {invalid_count}")
        return processed_features

  def insertData(self, geojson_paths, conn) -> None:
        
        cur = conn.cursor()
        schema_name = self.schema_name
        table_name = self.table_name
        
        features = self.__treatGeometry(geojson_paths)
        
        try:
            data_tuples = []
            for feature in features:
                # Extract properties and geometry from the feature
                properties = feature['properties']
                geometry = shape(feature['geometry'])  # Convert GeoJSON geometry to Shapely geometry
        
                # Prepare the data tuple for this feature
                data_tuple = (
                    feature['id'],                     # id
                    properties['uid'],                 # uid
                    properties['state'],               # state
                    properties['path_row'],            # path_row
                    properties['main_class'],          # main_class
                    properties['class_name'],          # class_name
                    properties['def_cloud'],           # def_cloud
                    properties['julian_day'],          # julian_day
                    properties['year'],                # year
                    properties['area_km'],             # area_km
                    properties['scene_id'],            # scene_id
                    properties['publish_year'],        # publish_year
                    properties['source'],              # source
                    properties['satellite'],           # satellite
                    properties['sensor'],             # sensor
                    properties['image_date'],         # image_date
                    geometry.wkt                      # Convert geometry to WKT
                )
                data_tuples.append(data_tuple)
        
            # SQL query to insert data
            insert_query = f"""
                INSERT INTO {schema_name}.{table_name} (
                    id,                -- id
                    uid,               -- uid
                    state,             -- state
                    path_row,          -- path_row
                    main_class,        -- main_class
                    class_name,       -- class_name
                    def_cloud,         -- def_cloud
                    julian_day,       -- julian_day
                    year,              -- year
                    area_km,           -- area_km
                    scene_id,          -- scene_id
                    publish_year,      -- publish_year
                    source,            -- source
                    satellite,         -- satellite
                    sensor,            -- sensor
                    image_date,        -- image_date
                    geometry           -- geometry
                )
                VALUES %s
                ON CONFLICT (id) DO NOTHING;  -- Skip duplicates based on id
            """
        
            # Execute the query using execute_values for batch insertion
            execute_values(cur, insert_query, data_tuples, template=None, page_size=100)
            conn.commit()
            cur.close()
            self.logger.info('Insert concluded successfully')
            
        except Exception as e:
            self.logger.error(f'insertData: {e}')

# end of class
if __name__ == '__main__':
    
    now = datetime.now().strftime("%H%M%S-%m.%d.%Y")
    if not os.path.exists("log"):
        os.makedirs("log")
    
    logging.basicConfig(
        filename=f"log//etl_terrabrasilis_{now}.log",
        filemode='a',
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )
    logger = logging.getLogger()
    
    if len(sys.argv) == 4:
        down=DownloadWFS(logger= logger, biome = sys.argv[1], sd = sys.argv[2], ed = sys.argv[3])
    else:
        down=DownloadWFS(logger= logger, biome = sys.argv[1])
        
    logger.info('Initializing process...')
    
    # Call download
    down.pagination()
    
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
    
    results = os.listdir('results')
    down.configPostgres(conn, os.path.join('results',results[0]))
    
    down.insertData(results, conn)
    shutil.rmtree('results')
    
    conn.close()
    












