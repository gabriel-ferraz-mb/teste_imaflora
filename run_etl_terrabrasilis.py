# -*- coding: utf-8 -*-
"""
Created on Sat Feb 22 11:18:03 2025

@author: Gabriel
"""

import sys
import logging
import datetime
from dotenv import find_dotenv, load_dotenv
import os
import json
import psycopg2
from sqlalchemy import create_engine

# os.chdir(r'C:\Projetos\teste_imaflora')

script_path = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_path)

from etl_terrabrasilis_class import EtlTerraBrasilis

if __name__ == '__main__':
    
    # Set up logging
    now = datetime.datetime.now().strftime("%H%M%S-%m.%d.%Y")
    
    if not os.path.exists("log"):
        os.makedirs("log")
    
    logging.basicConfig(
        filename=f"log//etl_terrabrasilis_{now}.log",
        filemode='a',
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )
    logger = logging.getLogger() 
    
    t = EtlTerraBrasilis(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], logger)
    
    logger.info('Initializing process...')
    
    with open("wfs_info.json", "r") as json_file:
        wfs_json_data = json.load(json_file)
    
    result = t.checkWsAndLayer(wfs_json_data)
    
    if not result['layer_exists']:
        logger.error('Layer does not exist. Exiting script.')
        sys.exit('Layer does not exist. Exiting script.')
    else:
        wfs_url = f"https://terrabrasilis.dpi.inpe.br/geoserver/{sys.argv[1]}/{sys.argv[2]}/wfs"
    
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
    
    # Create a SQLAlchemy engine
    db_url = f"postgresql+psycopg2://{os.getenv('USER')}:{os.getenv('PASSWORD')}@{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('DATABASE')}"
    engine = create_engine(db_url)
    
    table_name = f"{sys.argv[1]}_{sys.argv[2]}".replace("-", "_")
    
    t.configPostgres(cur, table_name, geometry)
    conn.commit()
    
    t.insertData(geometry, engine, table_name)
    
    # Close the cursor and connection
    cur.close()
    conn.close()
    logger.info("Process Concluded.")