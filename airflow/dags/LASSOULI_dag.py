from datetime import timedelta
from datetime import datetime
# L'objet DAG nous sert à instancier notre séquence de tâches.
from airflow import DAG

# On importe les Operators dont nous avons besoin.
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Les arguments qui suivent vont être attribués à chaque Operators.
# Il est bien évidemment possible de changer les arguments spécifiquement pour un Operators.
# Vous pouvez vous renseigner sur la Doc d'Airflow des différents paramètres que l'on peut définir.
default_args = {
    'owner': 'ilias',
    'email': ['lasselyase@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'mysql_conn_id': 'my_sql_db',
    # 'email_on_failure': False,
    # 'email_on_retry': False,
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

import requests
from io import StringIO
import pandas as pd

# Création du DAG
dag = DAG(
    'etl_LASSOULI',
    default_args=default_args,
    description='Airflow 101',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 11, 8),
    tags=['exemple']
)
###########################################################extract and transform task ################################################################################
def extract_and_transform(ti):

    r = requests.get(
    url='http://ladataverte.fr/api/1.0/data_points?id_indicators[]=1&id_indicators[]=4&childrensOf=13000000&type_place[]=country&from=2000-01-01&to=2021-12-31'
    )
    s=str(r.content,'utf-8')

    data = StringIO(s) 

    df=pd.read_csv(data)
    df = df.fillna(0)
    df = df.rename(columns={"Emission de CO2": "CO2"})
    df = df.rename(columns={"Température moyenne": "Temperature_moyenne"})
    df = df.astype('str')
    df['datetime'] = pd.to_datetime( df['datetime'] )
    df['CO2'] = df['CO2'].astype('float')
    df['Temperature_moyenne'] = df['Temperature_moyenne'].astype('float')
    print(df.dtypes)
    df.to_csv('/home/ilias/airflow/cellar/data.csv',index=False)
    ti.xcom_push(key='number_of_rows', value=df.shape[0])



t1 = PythonOperator(
    task_id='extract_and_transform',
    python_callable=extract_and_transform,
    dag=dag,
    do_xcom_push=True
)
#########################################################create database MYSQL task###############################################################################

from airflow.providers.mysql.operators.mysql import MySqlOperator


t2 = MySqlOperator(
    task_id='create_db_mysql',
    sql=r"""CREATE DATABASE IF NOT EXISTS TEST;""",
    dag=dag,
)
#################################################### transfer local files to server location ########################################################################
def dtype_mapping():
    return {'object' : 'TEXT',
        'int64' : 'INT',
        'float64' : 'FLOAT',
        'datetime64' : 'DATETIME',
        'bool' : 'TINYINT',
        'datetime64[ns]' : 'DATETIME',
        'category' : 'TEXT',
        'timedelta[ns]' : 'TEXT'}

def gen_tbl_cols_sql(df):
    dmap = dtype_mapping()
    sql = "pi_db_uid INT AUTO_INCREMENT PRIMARY KEY"
    df1 = df.rename(columns = {"" : "nocolname"})
    hdrs = df1.dtypes.index
    hdrs_list = [(hdr, str(df1[hdr].dtype)) for hdr in hdrs]
    for i, hl in enumerate(hdrs_list):
        sql += " ,{0} {1}".format(hl[0], dmap[hl[1]])
    return sql

def transfer_local_to_server(ti):

    df=pd.read_csv('/home/ilias/airflow/cellar/data.csv') 
    df.to_csv('/c/ProgramData/MySQL/MySQL Server 8.0/Data/test/data.csv',index=False)
    df['datetime'] = pd.to_datetime( df['datetime'] )
    tbl_cols_sql = gen_tbl_cols_sql(df)
    print(tbl_cols_sql.replace(',',',\n'))
    ti.xcom_push(key='nom_col_sql', value=tbl_cols_sql.replace(',',',\n'))


t3 = PythonOperator(
    task_id='transfer_local_to_server',
    python_callable=transfer_local_to_server,
    dag=dag,
    do_xcom_push=True
)

#########################################################create table in Database####################################################################

t4 = MySqlOperator(
    task_id='create_table_mysql_external_file',
    sql=r"""USE TEST;
    CREATE TABLE IF NOT EXISTS TEST_DATA(
    {{ task_instance.xcom_pull(key='nom_col_sql',task_ids='transfer_local_to_server') }}
    );
    LOAD DATA INFILE 'data.csv' INTO TABLE TEST_DATA
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (place, datetime, CO2, Temperature_moyenne)
    ;""",#.format(test=ti.xcom_pull(key='model_accuracy', task_ids=['training_model_A'])),
    dag=dag
)

######################################################### create table from query using MySqlHook ################################################################
from airflow.providers.mysql.hooks.mysql import MySqlHook

def create_table_query(**kwargs):


    hook = MySqlHook(mysql_conn_id='my_sql_db', schema='TEST')
    sql = "CREATE TABLE IF NOT EXISTS FRANCE_DATA select * from TEST_DATA where place='FRANCE'"
    result = hook.get_first(sql)
    sql = "select * from TEST_DATA where place='FRANCE'"
    df = hook.get_pandas_df(sql)
    j= df.head(10).to_json(orient='records')

    return j
    

t5 = PythonOperator(
    task_id='create_table_query',
    python_callable=create_table_query,
    dag=dag,
)

####################################################### drop table if new record are in csv file ###################################################################


def drop_if(ti):

    hook = MySqlHook(mysql_conn_id='my_sql_db', schema='TEST')
    sql = "SELECT count(*) FROM TEST_DATA"
    result = hook.get_first(sql)

    print(type(result[0]))

    nb_rows = ti.xcom_pull(key='number_of_rows',task_ids='extract_and_transform')
    print(type(nb_rows))

    if nb_rows > result[0]:

        sql = "DROP TABLE IF EXISTS TEST_DATA; DROP TABLE IF EXISTS FRANCE_DATA "
    
    return {'bool':nb_rows > result[0] , 'nb_rows_sql': result[0] , 'nb_rows_csv': nb_rows}


t6 = PythonOperator(
    task_id='drop_if',
    python_callable=drop_if,
    dag=dag,
)


t1 >> t2 >> t3 >> t4 >> t5 

t1 >> t6 >> t3 >> t4
