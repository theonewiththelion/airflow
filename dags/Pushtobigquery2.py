from re import A
from ssl import ALERT_DESCRIPTION_RECORD_OVERFLOW
import pandas as pd
import pandas_gbq
import requests
import json

from datetime import *
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Import the BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago



#Airflow arguments
default_args = {
    'owner': 'admin',
    'email': 'jonathan.dejesus.azor@gmail.com',
    'start_date': datetime(2022,8,28),    
    
    # 'depends_on_past': False,
    # 'start_date': days_ago(2),
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 1,
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
#Extract ------Call API-----------------------------------------------------------
#Api location - https://data.austintexas.gov/Public-Safety/Crime-Reports/fdj4-gpfu

#Extract ------
def api_extraction():
    #So I can use variable into another functions 
    #global filter_df
    
    #Table contain around 2,428,732 records, please keep it limited due to resources (Data Studio)
    austin_crime_reports_api = requests.get("https://data.austintexas.gov/resource/fdj4-gpfu.json?$limit=200")
#------------------------------------------------
    #Save API into a variable
    austin_crime = austin_crime_reports_api.text

    #Convert variable austin_crime into JSON format
    json.loads(austin_crime)

    #Save Json file into a dataframe
    df_austin_crime = pd.read_json(austin_crime)

    #Add specific fields into a another DataFrame = filter_df
    filter_df = df_austin_crime[["incident_report_number", 'crime_type','location_type', 
                                "address","zip_code","occ_date","occ_time","rep_date","rep_time"]]
    #print(filter_df)

    return filter_df

   
#TRANSFORM --------------------------------------------------------------------
def crime_stats_transformation(filter_df):
    #Rename Columns
    #Documenation on renaming dataframe: https://stackabuse.com/how-to-rename-pandas-dataframe-column-in-python/
    filter_df.rename(columns = {'occ_date' : 'occurred_date', 
                                'occ_time' : 'ocurred_time', 
                                'rep_date' : 'reported_date', 
                                'rep_time' : 'reported_time'}, inplace= True)
    #Change the Format of Ocurred Time
    #Documentation 
    #https://stackoverflow.com/questions/65197597/how-to-convert-military-time-format-e-g-1305-to-hhmm-e-g-1305-in-python
    time = pd.to_datetime(filter_df['ocurred_time'].astype(str).str.zfill(4), format='%H%M').dt.time
    #Add the variable time into the dataframe
    #Documenation = https://github.com/wesm/feather/issues/349
    filter_df['new_occurred_time'] = time
    #Change the type of the field "new ocurred time"
    filter_df['new_occurred_time'] = filter_df['new_occurred_time'].astype(str)
    #Change the time of the field "new ocurred time" from Military time to Standard time
    filter_df['std_time_new_occurred_time'] = [datetime.strptime(t, "%H:%M:%S").strftime("%I:%M %p") for t in filter_df['new_occurred_time']]
    #Change the format of the field "Reported time"
    #Documentation 
    #https://stackoverflow.com/questions/65197597/how-to-convert-military-time-format-e-g-1305-to-hhmm-e-g-1305-in-python
    r_time = pd.to_datetime(filter_df['reported_time'].astype(str).str.zfill(4), format='%H%M').dt.time
    #Add the variable time into the dataframe
    #Documenation
    #https://github.com/wesm/feather/issues/349
    filter_df['new_reported_time'] = r_time
    #Change the type of the field "new ocurred time"
    filter_df['new_reported_time'] = filter_df['new_reported_time'].astype(str)
    #Change the time of the field "new reported time" from Military time to Standard time
    filter_df['std_time_new_reported_time'] = [datetime.strptime(t, "%H:%M:%S").strftime("%I:%M %p") for t in filter_df['new_reported_time']]
    
    return filter_df
#Load --------------------------------------------------------------------
def crime_stats_load_intobigquery(filter_df):
    #Documentation 
    #https://pandas-gbq.readthedocs.io/en/latest/writing.html
    project_id = 'crafty-shield-359406'
    table_id = 'data_id_id.table_test'
    pandas_gbq.to_gbq(filter_df, 'data_id_id.table_test', project_id='crafty-shield-359406', if_exists='replace')
    print("Done?, please check Big Query")

dag = DAG(
    'ETL_Examples',
    default_args=default_args,
    description='Running workflow for Crime in Austin project',
    #Run every 2 mins 
    schedule_interval=timedelta(minutes=20)
)


# define Task
t1 = PythonOperator(
    task_id='Extraction',
    python_callable= api_extraction,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)

t2 = PythonOperator(
    task_id='Transform',
    #provide_context=True
    python_callable= crime_stats_transformation,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)

t3 = PythonOperator(
    task_id='Load',
    #provide_context=True
    python_callable= crime_stats_load_intobigquery,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)

#setting up Dependencies
#t1 >> t2 >> t3



#api_extraction() #Extract records from the API 
#crime_stats_transformation() #Prepare the data for transformation
#crime_stats_load_intobigquery() #Load Data into Big Query 

#LEARN THE LOGIC OF THIS SHIET
#Documtetation https://stackoverflow.com/questions/47362844/using-an-input-variable-in-multiple-functions-in-python
crime_stats_load_intobigquery(crime_stats_transformation(api_extraction()))



#big Query link: