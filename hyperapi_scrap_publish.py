#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas
import os
import io
import requests
import json
from pathlib import Path
from datetime import datetime
import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, TableName
from tableauhyperapi import escape_string_literal
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_projects_dataframe


# In[2]:


# gathering data from api
api_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/forecast?aggregateHours=1&combinationMethod=aggregate&includeAstronomy=true&contentType=csv&unitGroup=metric&locationMode=single&key=ZD8LFMAZYH79RN3D8CQLKC2E8&dataElements=default&locations=chennai"
response = requests.get(api_url)
data = response.text

# reading api request as a pandas datafrme

df = pandas.read_csv(io.StringIO(data))

df = df[['Name',
 'Date time',
 'Temperature',
 'Chance Precipitation (%)',
 'Precipitation',
 'Wind Speed',
 'Wind Gust',
 'Visibility',
 'Cloud Cover',
 'Relative Humidity',
 'Moon Phase',
 'Conditions']]
df['Date time'] = df['Date time'].apply(pandas.to_datetime)
df['Conditions'] = df['Conditions'].fillna('null')


# In[3]:


# Step 1 : defining path for hyperfile

path_to_hyper = 'weather_forcast.hyper'


# 
with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, 'myapp' ) as hyper:

# Step 2:  Create the the .hyper file, replace it if it already exists
    with Connection(endpoint=hyper.endpoint, 
                    create_mode=CreateMode.CREATE_AND_REPLACE,
                    database=path_to_hyper) as connection:

# Step 3: Create the schema
        connection.catalog.create_schema('Extract')

# Step 4: Create the table definition
        schema = TableDefinition(table_name=TableName('Extract','Extract'),
            columns=[
            TableDefinition.Column('name', SqlType.text()),
            TableDefinition.Column('date', SqlType.date()),
            TableDefinition.Column('temperature', SqlType.double()),
            TableDefinition.Column('chance_precipitation', SqlType.double()),
            TableDefinition.Column('precipitation', SqlType.double()),
            TableDefinition.Column('wind_speed', SqlType.double()),
            TableDefinition.Column('wind_gust', SqlType.double()),
            TableDefinition.Column('visiblity', SqlType.double()),
            TableDefinition.Column('cloud_cover', SqlType.double()),
            TableDefinition.Column('relative_humidity', SqlType.double()),
            TableDefinition.Column('moon_phase', SqlType.double()),
            TableDefinition.Column('condition', SqlType.text()),
         ])
    
# Step 5: Create the table in the connection catalog
        connection.catalog.create_table(schema)
    
        with Inserter(connection, schema) as inserter:
            for index, row in df.iterrows():
                inserter.add_row(row)
            inserter.execute()

    print("The connection to the Hyper file is closed.")


# In[4]:


#config to publish
hyper_name = 'weather_forcast.hyper'
server_address = 'https://prod-apnortheast-a.online.tableau.com/'
site_name = 'demo1996'
project_name = 'api_test'
token_name = 'mytoken'
token_value = 'xrGvy7KFRmyPBoXSezZ/vg==:7sGeIgW3yuKsE1ouoBgytcg2ZCuKoMVb'


# In[5]:


# Publishing the hyper file to the server

def publish_hyper():
    """
    Shows how to leverage the Tableau Server Client (TSC) to sign in and publish an extract directly to Tableau Online/Server
    """

    # Sign in to server
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name=token_name, personal_access_token=token_value, site_id=site_name)
    server = TSC.Server(server_address, use_server_version=True)
    
    print(f"Signing into {site_name} at {server_address}")
    with server.auth.sign_in(tableau_auth):
        # Define publish mode - Overwrite, Append, or CreateNew
        publish_mode = TSC.Server.PublishMode.Overwrite
        
        # Get project_id from project_name
        all_projects, pagination_item = server.projects.get()
        for project in TSC.Pager(server.projects):
            if project.name == project_name:
                project_id = project.id
    
        # Create the datasource object with the project_id
        datasource = TSC.DatasourceItem(project_id)
        
        print(f"Publishing {hyper_name} to {project_name}...")
        # Publish datasource
        datasource = server.datasources.publish(datasource, path_to_hyper, publish_mode)
        print("Datasource published. Datasource ID: {0}".format(datasource.id))


if __name__ == '__main__':
    publish_hyper()


# In[ ]:




