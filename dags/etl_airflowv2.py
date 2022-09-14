from re import A
from ssl import ALERT_DESCRIPTION_RECORD_OVERFLOW
import pandas as pd
import pandas_gbq
import requests
import json
from datetime import *

#The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
#Task groups
#Documentation https://www.astronomer.io/guides/task-groups/
from airflow.utils.task_group import TaskGroup



#Create ETL
#Extraction - get data from API endpoint and store in a table
#Transformation - get data from table and perform transformation
#Load - Get from old table to new table

#Add Dag - arguments
#Airflow arguments
default_args = {
    'owner': 'admin',
    'email': 'jonathan.dejesus.azor@gmail.com',
    'start_date': datetime(2022,9,3),    
    
    # 'depends_on_past': False,
    # 'start_date': days_ago(2),
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

#Extraction
def api_extraction():

    #Table contain around 2,428,732 records, please keep it limited due to resources (Data Studio)
    austin_crime_reports_api = requests.get("https://data.austintexas.gov/resource/fdj4-gpfu.json?$limit=10")
#------------------------------------------------
    #Save API into a variable
    austin_crime = austin_crime_reports_api.text

    #Convert variable austin_crime into JSON format
    json.loads(austin_crime)

    #Save Json file into a dataframe
    df_austin_crime = pd.read_json(austin_crime)

    #Add specific fields into a another DataFrame = filter_df
    stg_austin_crime = df_austin_crime[["incident_report_number", 'crime_type','location_type', 
                                "address","zip_code","occ_date","occ_time","rep_date","rep_time"]]

  #Load data into straging table "staging_table"
    #https://pandas-gbq.readthedocs.io/en/latest/writing.html
    project_id = 'crafty-shield-359406'
    table_id = 'staging_table.staging_table'
    pandas_gbq.to_gbq(stg_austin_crime, table_id, project_id='crafty-shield-359406', if_exists='replace')
    print("Extraction of API and load into Staging - Completed")

#Transformation
def transformation_rename():
    #get data from the staging table "staging_table"
    #Documentation on how to extract data from big query table
    #https://pandas-gbq.readthedocs.io/en/latest/reading.html
    sql = """
    SELECT *
    FROM `crafty-shield-359406.staging_table.staging_table`
    """
    #Store data from the staging table to variable/df df_transformed_austin_crime
    df_transformed_austin_crime = pandas_gbq.read_gbq(sql, project_id='crafty-shield-359406')
  
    #Rename columns/fields in the dataframe
    #Documenation on renaming dataframe: https://stackabuse.com/how-to-rename-pandas-dataframe-column-in-python/
    df_transformed_austin_crime.rename(columns = {'occ_date' : 'occurred_date', 
                                'occ_time' : 'ocurred_time', 
                                'rep_date' : 'reported_date', 
                                'rep_time' : 'reported_time'}, inplace= True)

    #Load data into straging area
    #https://pandas-gbq.readthedocs.io/en/latest/writing.html
    project_id = 'crafty-shield-359406'
    table_id = 'staging_table.staging_table2'
    pandas_gbq.to_gbq(df_transformed_austin_crime, table_id, project_id='crafty-shield-359406', if_exists='replace')
    print("Field renamed and loaded into staging 2 - completed")

def transformation_date_ocurred():
    #get data from the staging table "staging_table"
    #Documentation on how to extract data from big query table
    sql = """
    SELECT *
    FROM `crafty-shield-359406.staging_table.staging_table2`
    """
    #Store data from the staging table to dataframe/variable df_transformed_austin_crime1
    df_transformed_austin_crime1 = pandas_gbq.read_gbq(sql, project_id='crafty-shield-359406')
    ################FUNCTION TO CHANGE THE DATE
    #################Change the Format of Ocurred Time
    #Documentation https://stackoverflow.com/questions/65197597/how-to-convert-military-time-format-e-g-1305-to-hhmm-e-g-1305-in-python
    time = pd.to_datetime(df_transformed_austin_crime1['ocurred_time'].astype(str).str.zfill(4), format='%H%M').dt.time
    #Add the variable time into the dataframe
    #Documenation = https://github.com/wesm/feather/issues/349
    df_transformed_austin_crime1['new_occurred_time'] = time
    #Change the type of the field "new ocurred time"
    df_transformed_austin_crime1['new_occurred_time'] = df_transformed_austin_crime1['new_occurred_time'].astype(str)
    #Change the time of the field "new ocurred time" from Military time to Standard time
    df_transformed_austin_crime1['std_time_new_occurred_time'] = [datetime.strptime(t, "%H:%M:%S").strftime("%I:%M %p") for t in 
                                                                                df_transformed_austin_crime1['new_occurred_time']]
    #Load data into staging area
    #https://pandas-gbq.readthedocs.io/en/latest/writing.html
    project_id = 'crafty-shield-359406'
    table_id = 'staging_table.staging_table2'
    pandas_gbq.to_gbq(df_transformed_austin_crime1, table_id, project_id='crafty-shield-359406', if_exists='replace')
    print("Field Ocurred time fixed, and loaded into staging area 2 - completed")

def transformation_date_reported():
    #get data from the staging table stg_austin_crime
    #Documentation on how to extract data from big query table
    #https://pandas-gbq.readthedocs.io/en/latest/reading.html
    sql = """
    SELECT *
    FROM `crafty-shield-359406.staging_table.staging_table2`
    """
    #Store data from the staging table stg_austin_crime
    df_transformed_austin_crime1 = pandas_gbq.read_gbq(sql, project_id='crafty-shield-359406')
    ##################Change the format of the field "Reported time"
    #Documentation 
    #https://stackoverflow.com/questions/65197597/how-to-convert-military-time-format-e-g-1305-to-hhmm-e-g-1305-in-python
    r_time = pd.to_datetime(df_transformed_austin_crime1['reported_time'].astype(str).str.zfill(4), format='%H%M').dt.time
    
    #Add the variable time into the dataframe
    #Documenation
    #https://github.com/wesm/feather/issues/349
    df_transformed_austin_crime1['new_reported_time'] = r_time
    #Change the type of the field "new ocurred time"
    df_transformed_austin_crime1['new_reported_time'] = df_transformed_austin_crime1['new_reported_time'].astype(str)
    #Change the time of the field "new reported time" from Military time to Standard time
    df_transformed_austin_crime1['std_time_new_reported_time'] = [datetime.strptime(t, "%H:%M:%S").strftime("%I:%M %p") for t in 
                                                                            df_transformed_austin_crime1['new_reported_time']]
    #Load data into staging area
    #https://pandas-gbq.readthedocs.io/en/latest/writing.html
    project_id = 'crafty-shield-359406'
    table_id = 'staging_table.staging_table2'
    pandas_gbq.to_gbq(df_transformed_austin_crime1, table_id, project_id='crafty-shield-359406', if_exists='replace')
    print("Reported time fixed, and loaded into staging area 2 - completed")


    #defi to drop columns
    #Drop columns
    #created new columns, no need for this two
    #df_transformed_austin_crime1 = df_transformed_austin_crime1.drop(columns=['new_occurred_time', 'new_reported_time'])
#Load
def load_data():
    #get data from the staging table 
    #Documentation on how to extract data from big query table
    #https://pandas-gbq.readthedocs.io/en/latest/reading.html
    sql = """
    SELECT *
    FROM `crafty-shield-359406.staging_table.staging_table2`
    """
    #Store data from the staging table stg_austin_crime
    df_austin_crime = pandas_gbq.read_gbq(sql, project_id='crafty-shield-359406')

    #Load into main -query table
    #https://pandas-gbq.readthedocs.io/en/latest/writing.html
    project_id = 'crafty-shield-359406'
    table_id = 'austin_table.z_crime'
    pandas_gbq.to_gbq(df_austin_crime, table_id, project_id='crafty-shield-359406', if_exists='replace')
    print("Done, please check the table")

#change this for a with ....
dag = DAG(
    'etl_airflow',
    default_args=default_args,
    description='Running workflow for Crime in Austin project',
    #Run every 2 mins 
    schedule_interval='@once'
    #timedelta(minutes=20)
)

#define Task
start = DummyOperator(
    task_id='start',
    dag=dag)



t1 = PythonOperator(
    task_id='Extraction',
    python_callable= api_extraction,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)

transform = DummyOperator(
    task_id='transform',
    dag=dag)

t2 = PythonOperator(
    task_id='field_rename',
    #provide_context=True
    python_callable= transformation_rename,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,)

t5 = PythonOperator(
    task_id='date_formatting_ocurred',
    #provide_context=True
    python_callable= transformation_date_ocurred,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)

t4 = PythonOperator(
    task_id='date_formatting_reported',
    #provide_context=True
    python_callable= transformation_date_reported,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)

t3 = PythonOperator(
    task_id='Load',
    #provide_context=True
    python_callable= load_data,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)

completed = DummyOperator(
    task_id='completed',
    dag=dag)

#setting up Dependencies
#Documentation - https://cloud.google.com/composer/docs/grouping-tasks-inside-dags#airflow-2
start >> t1 >> transform >> t2 >> [t5,t4] >> t3 >> completed

# api_extraction()
# transformation_rename()
# transformation_date_ocurred()
# transformation_date_reported()
# load_data()

print("Dag Completed - Please check airflow website")
print(" & Congrats on your first ETL with airflow")
#Remember to drop not needed tables

# api_extraction()
# transformation()
# load_data()

