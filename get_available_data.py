# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 21:13:21 2025

@author: Gabriel
"""

import json
from owslib.wfs import WebFeatureService
import os

directory = r'C:\Projetos\teste_imaflora'
os.chdir(directory)

# Initialize the root WFS
wfs_url = "https://terrabrasilis.dpi.inpe.br/geoserver/wfs"
root_wfs = WebFeatureService(url=wfs_url, version="1.1.0")

# Dictionary to store the information
wfs_info = {}

# Iterate through the layers
workspaces = list(root_wfs.contents)
for workspace in workspaces:
    root1 = workspace.split(":")[0]
    sub_url = f"https://terrabrasilis.dpi.inpe.br/geoserver/{root1}/wfs"
    
    # Access the sub WFS
    wfs = WebFeatureService(url=sub_url, version="1.1.0")
    
    # Store the layer information in the dictionary
    wfs_info[root1] = {
        "url": sub_url,
        "layers": {}
    }
    
    # Iterate through each content in the sub WFS
    for layer in list(wfs.contents):
        root2 = layer.split(":")[1]
        try:
            # Get the schema for the current content
            schema = wfs.get_schema(layer)
            wfs_info[root1]["layers"][root2] = {
                "schema": schema
            }
        except Exception as e:
            # Handle cases where schema retrieval fails
            wfs_info[root1]["layers"][root2] = {
                "schema": None,
                "error": str(e)
            }

# Convert the dictionary to a JSON string
wfs_json = json.dumps(wfs_info, indent=4)

# Optionally, save the JSON to a file
with open("wfs_info.json", "w") as json_file:
    json_file.write(wfs_json)

