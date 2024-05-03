!pip install azure-storage-blob
!pip install psycopg2 sqlalchemy

import pandas as pd
import numpy as np
import json
import requests
from io import StringIO
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from math import ceil
import datetime
import calendar
from sqlalchemy import create_engine


#specify the path to your JSON configuration file
config_file_path = 'config/config.json'

#load the JSON configuration file
with open(config_file_path, 'r') as config_file
    config = json.load(config_file)

#print the configuration 
ConnectionString = config['connectionString']

# Azure Functions
def azure_upload_blob(connect_str, container_name, blob_name, data):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded to Azure Blob: {blob_name}")

def azure_download_blob(connect_str, container_name, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    download_stream = blob_client.download_blob()
    return download_stream.readall().decode('utf-8')
#Uploading Data into Azure Container
#specify the path to your JSON configuration file
config_file_path = 'config.json'

#load the JSON configuration file
with open(config_file_path, 'r') as config_file:
    config = json.load(config_file)

#print the configuration
Connection_STRING = config['CONNECTION_STRING_AZURE_STORAGE']

CONNECTION_STRING_AZURE_STORAGE = config['connectionString']
CONTAINER_AZURE = 'studentattendance'
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING_AZURE_STORAGE)
container_client = blob_service_client.get_container_client(CONTAINER_AZURE)

attendance_df = pd.DataFrame()

#List all blobs in the specified container
blob_list = container_client.list_blobs()
for blob in blob_list:
  print(blob_name)
  blob_client = container_client.get_blob_client(blob=blob.name)
  blob_data = blob_client.download_blob()
  blob_content = blob_data.readall().decode('utf-8')
  df = pd.read_csv(StringIO(blob_content))
  attendance_df = df.copy


#Generic Functions

def create_string(length):
  if isinstance(length, int) and length > 0:
    result_string = "(" + "?," * (length - 1) + "?)"

def insert_data(table_name, df):
  conn = pyodbc.connect(connection_string)
  cursor = conn.cursor()
  result = create_string(len(df.columns))
  #Insert Data into table
  insert_query = f"INSERT INTO {table_name} VALUES {result}"
  print(insert_query)
  cursor.executemany(insert_query, df.values.tolist())
  conn.commit()
  conn.close()
dim_school_df = attendance_df.copy()
table_name = dim_school
result = create_string(len(dim_school_df.columns))
insert_data(table_name, dim_school_df)

azure_download_blob(CONNECTION_STRING_AZURE_STORAGE, CONTAINER_AZURE, 'nycatt_chunk_'+i+'.csv')
#Downloading Data from Azure Container into Dataframe
config_file_path = 'config.json'

#load the JSON configuration file
with open(config_file_path, 'r') as config_file:
    config = json.load(config_file)

#print the configuration
#Connection_STRING = config['CONNECTION_STRING_AZURE_STORAGE']

CONNECTION_STRING_AZURE_STORAGE = config['connectionString']
CONTAINER_AZURE = 'studentattendance'
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING_AZURE_STORAGE)
container_client = blob_service_client.get_container_client(CONTAINER_AZURE)
data = []
blob_list = container_client.list_blobs()
for blob in blob_list:
  blob_client = container_client.get_blob_client(blob = blob.name)
  blob_data = blob_client.download_blob()
  blob_content = blob_data.readall().decode('utf-8')
  df = pd.read_csv(StringIO(blob_content))
  data.append(df)
all_data = pd.concat(data, ignore_index=True)
nyc_df = all_data.copy()
#Cleaning Dataset
nyc_df.head()
nyc_df.rename(columns={'chronically_absent_1': 'chronic_absent_percentage'}, inplace=True)
nyc_df = nyc_df[(nyc_df.attendance != 's') & (nyc_df.chronically_absent != 's') & (nyc_df.chronic_absent_percentage != 's')]
nyc_df['attendance'] = nyc_df['attendance'].astype(float)
nyc_df['chronic_absent_percentage'] = nyc_df['chronic_absent_percentage'].astype(float)
nyc_df['attendance'] = (nyc_df['attendance'] / 100)
nyc_df['chronic_absent_percentage'] = (nyc_df['chronic_absent_percentage'] / 100)
nyc_df = nyc_df.drop(columns=['contributing_20_total_days'])
nyc_df.isnull().values.any()
nyc_df.head()

#Creating Mappings for Dimension Tables
#creating Demographic Category Dimension

dem_cat_mapping = {
    'All Students': 1,
    'SWD Status': 2,
    'Ethnicity': 3,
    'Gender': 4,
    'Poverty': 5,
    'ELL Status': 6
}
unique_ids = nyc_df.demographic_category.unique()
dem_cat_df = pd.DataFrame(unique_ids, columns = ['demographic_category'])
dem_cat_df['demographic_category_id'] = dem_cat_df['demographic_category'].map(dem_cat_mapping)
neworder = ['demographic_category_id','demographic_category']
dem_cat_df = dem_cat_df[neworder]
nyc_df['demographic_category_id'] = nyc_df['demographic_category'].map(dem_cat_mapping)
#creating Demographic Variable Dimension

dem_var_mapping = {
    'All Students': 1,
    'SWD': 2,
    'Not SWD': 3,
    'Hispanic': 4,
    'White': 5,
    'Asian': 6,
    'Black': 7,
    'Other': 8,
    'Female': 9,
    'Male': 10,
    'Not Poverty': 11,
    'Poverty': 12,
    'ELL': 13,
    'Not ELL': 14
}
unique_var_ids = nyc_df.demographic_variable.unique()
dem_var_df = pd.DataFrame(unique_var_ids, columns = ['demographic_variable'])
dem_var_df['demographic_variable_id'] = dem_var_df['demographic_variable'].map(dem_var_mapping)
neworder1 = ['demographic_variable_id','demographic_variable']
dem_var_df = dem_var_df[neworder1]
nyc_df['demographic_variable_id'] = nyc_df['demographic_variable'].map(dem_var_mapping)
#creating Time Dimension

year_mapping = {
    '2013-14': 1,
    '2014-15': 2,
    '2015-16': 3,
    '2016-17': 4,
    '2017-18': 5,
    '2018-19': 6,
}
unique_time_ids = nyc_df.year.unique()
time_df = pd.DataFrame(unique_time_ids, columns = ['year_number'])
time_df['time_id'] = time_df['year_number'].map(year_mapping)
neworder2 = ['year_number','time_id']
time_df = time_df[neworder2]
nyc_df = nyc_df.rename(columns={'year':'year_number'})
nyc_df['time_id'] = nyc_df['year_number'].map(year_mapping)
#creating Grade Dimension

grade_mapping = {
    'All Grades': 1,
    'PK in K-12 Schools': 2,
    '0K': 3,
    '1': 4,
    '2': 5,
    '3': 6,
    '4': 7,
    '5': 8,
    '6': 9,
    '7': 10,
    '8': 11,
    '9': 12,
    '10': 13,
    '11': 14,
    '12': 15,
}
unique_grade_ids = nyc_df.grade.unique()
grade_df = pd.DataFrame(unique_grade_ids, columns = ['grade'])
grade_df['grade_id'] = grade_df['grade'].map(grade_mapping)
neworder3 = ['grade_id','grade']
grade_df = grade_df[neworder3]
nyc_df['grade_id'] = nyc_df['grade'].map(grade_mapping)
#Creating School Dimension

unique_ids = nyc_df.dbn.unique()
neworder4 = ['dbn','school_name']
school = nyc_df.groupby(['dbn','school_name']).apply(list)
school = school.reset_index()
school_df = pd.DataFrame(school)
school_df2 = school_df[neworder4]
nyc_df['attendance_id'] = range(1, len(nyc_df)+1)
nyc_df = nyc_df.rename(columns={'attendance':'attendance_percentage'})
new_order = ['attendance_id', 'total_days', 'days_present','days_absent', 'attendance_percentage', 'chronically_absent', 'chronic_absent_percentage', 'demographic_category_id', 'time_id', 'dbn', 'grade_id', 'demographic_variable_id']
nyc_df = nyc_df[new_order]
nyc_df = nyc_df.round({'attendance_percentage': 3, 'chronic_absent_percentage': 3})

nyc_df.head()
#creating connection with database
pwd = '*****'
database_url = f'postgresql://jackd:{pwd}@cis9440baruch.postgres.database.azure.com/postgres'

#create SQLAlchemy engine
engine = create_engine(database_url)
nyc_df.to_sql('facts_attendance',con=engine, schema= 'nycattendance', if_exists= 'append', index = False)
with engine.connect() as conn:
    grade_df.to_sql('dim_grade', con=conn, if_exists='append', index=False)
    conn.commit()  # Commit the transaction
nyc_df.to_csv('facts_attendance.csv',index=False)
school_df2.to_csv('dim_school.csv',index=False)
grade_df.to_csv('dim_grade.csv',index=False)
time_df.to_csv('dim_time.csv',index=False)
dem_var_df.to_csv('dim_demographic_variable.csv',index=False)
dem_cat_df.to_csv('dim_demographic_category.csv',index=False)