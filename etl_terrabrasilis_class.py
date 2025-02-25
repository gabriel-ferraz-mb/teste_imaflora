# -*- coding: utf-8 -*-
"""
Created on Sat Feb 22 11:18:03 2025

@author: Gabriel
"""

from owslib.fes import PropertyIsGreaterThanOrEqualTo, PropertyIsLessThanOrEqualTo, And
from owslib.etree import etree
from owslib.wfs import WebFeatureService
from psycopg2 import sql
from psycopg2.extensions import cursor as Cursor
import geopandas as gpd
import pandas as pd
from shapely.validation import make_valid
import logging
import time
from sqlalchemy import engine
from typing import Dict, Any, Optional, Union, List

class EtlTerraBrasilis:
    def __init__(self, workspace: str, layer_name: str, start: str, end: str, logger: logging.Logger) -> None:
        self.start: str = start
        self.end: str = end
        self.workspace: str = workspace
        self.layer_name: str = layer_name
        self.logger: logging.Logger = logger  # Pass the logger object
        
    def checkWsAndLayer(self, json_data: Dict[str, Any]) -> Dict[str, Union[bool, str]]:
        ws: str = self.workspace
        layer: str = self.layer_name
        
        if ws not in json_data:
            self.logger.error(f"Workspace '{ws}' not found.")
            return {
                "ws_exists": False,
                "layer_exists": False,
                "has_year_column": False,
                "message": f"Workspace '{ws}' not found."
            }
        
        if layer not in json_data[ws]["layers"]:
            self.logger.error(f"Content '{layer}' not found in workspace '{ws}'.")
            return {
                "ws_exists": True,
                "layer_exists": False,
                "has_year_column": False,
                "message": f"Content '{layer}' not found in workspace '{ws}'."
            }
        
        schema: Optional[Dict[str, Any]] = json_data[ws]["layers"][layer].get("schema")
        
        if schema and "properties" in schema and "year" in schema["properties"]:
            self.logger.info(f"Layer '{layer}' in workspace '{ws}' has a 'year' column.")
            return {
                "ws_exists": True,
                "layer_exists": True,
                "has_year_column": True,
                "message": f"Layer '{layer}' in workspace '{ws}' has a 'year' column."
            }
        else:
            self.logger.warning(f"Layer '{layer}' in workspace '{ws}' does not have a 'year' column.")
            return {
                "ws_exists": True,
                "layer_exists": True,
                "has_year_column": False,
                "message": f"Layer '{layer}' in workspace '{ws}' does not have a 'year' column."
            }

    def callWfs(self, result: Dict[str, Union[bool, str]], wfs_url: str) -> Any:
        wfs: WebFeatureService = WebFeatureService(url=wfs_url, version="1.1.0")        
        
        max_attempts: int = 100
        attempt: int = 0
        
        while attempt < max_attempts:
            try:
                if result['has_year_column']:
                    filter_start = PropertyIsGreaterThanOrEqualTo(propertyname='year', literal=self.start)
                    filter_end = PropertyIsLessThanOrEqualTo(propertyname='year', literal=self.end)
                    
                    combined_filter = And([filter_start, filter_end])
                    
                    filterxml: str = etree.tostring(combined_filter.toXML()).decode("utf-8")
                    response: Any = wfs.getfeature(typename=self.layer_name, filter=filterxml, outputFormat='application/json')
                else:
                    response: Any = wfs.getfeature(typename=self.layer_name, outputFormat='application/json')
                return response
            except Exception as e:
                attempt += 1
                self.logger.error(f"Attempt {attempt} failed: {e}")
                time.sleep(10)
        
        raise Exception("Failed to connect after 100 attempts")
        
    def treatGeometry(self, response: Any) -> gpd.GeoDataFrame:  
        try:
            gdf: gpd.GeoDataFrame = gpd.read_file(response)
            
            self.logger.info("Checking for invalid geometries")
            invalid: pd.Series = ~gdf.geometry.is_valid
            
            valid_count: int = gdf.geometry.is_valid.sum()
            self.logger.info(f"Valid geometries count: {valid_count}")
            
            if invalid.any():
                self.logger.warning(f"Found {invalid.sum()} invalid geometries. Fixing them")
                gdf.loc[invalid, 'geometry'] = gdf.loc[invalid, 'geometry'].apply(make_valid)
            
            self.logger.info("Reprojecting to EPSG:3857")
            gdf: gpd.GeoDataFrame = gdf.to_crs(epsg=3857)
            
            return gdf
        
        except Exception as e:
            self.logger.error(f'treatGeometry: {e}')
            
    def mapDtypeToPg(self, pandas_dtype) -> str:
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
        
    def configPostgres(self, cur: Cursor, table_name: str, gdf: gpd.GeoDataFrame) -> None:
        try:
            schema_name: str = 'raw_data'
            
            cur.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';")
            schema_exists: Optional[tuple] = cur.fetchone()
            
            if not schema_exists:
                cur.execute(sql.SQL("CREATE SCHEMA {schema_name};"))
                self.logger.info(f"Schema '{schema_name}' created.")
            
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_extension 
                    WHERE extname = 'postgis'
                );
            """)
            postgis_installed: bool = cur.fetchone()[0]
            
            if not postgis_installed:
                cur.execute("CREATE EXTENSION postgis;")
                self.logger.info("PostGIS extension installed.")
            
            cur.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{schema_name}' 
                    AND table_name = '{table_name}'
                );
            """)
            table_exists: bool = cur.fetchone()[0]
            
            if not table_exists:
                columns: List[str] = []
                for col in gdf.columns:
                    if col == 'geometry':
                        columns.append(f"{col} GEOMETRY(Geometry, 3857)")
                    else:
                        pg_dtype: str = self.mapDtypeToPg(gdf[col].dtype)
                        columns.append(f"{col} {pg_dtype}")
                
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
                    cur.execute(f"""
                        CREATE INDEX idx_geometry_{table_name} ON {schema_name}.{table_name} USING GIST (geometry);
                    """)
                    self.logger.info("Indexes created on the table.")
                except Exception as e:
                    self.logger.error(f"Error creating table: {e}")
            
        except Exception as e:
            self.logger.error(f'configPostgres: {e}')
    
    def insertData(self, gdf: gpd.GeoDataFrame, engine: engine.Engine, table_name: str) -> None:
        schema_name: str = 'raw_data'
        try:
            gdf.to_postgis(
                name=table_name,
                con=engine,
                schema=schema_name,
                if_exists='append',
                index=False,
            )
            self.logger.info(f"Data inserted into table '{schema_name}.{table_name}'.")
                
        except Exception as e:
            self.logger.error(f'insertData: {e}')
        

