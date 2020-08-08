from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import psycopg2
import requests


dag_second_assignment = DAG(
	dag_id = 'second_assignment',
	start_date = datetime(2020,8,14), # 적당히 조절
	schedule_interval = '0 2 * * *')  # 적당히 조절

# Redshift connection 함수
def get_Redshift_connection():
    host = "grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = ""
    redshift_pass = ""
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()

def etl():
	# Extract
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
	result = requests.get(link)lines = result.text.split("\n")
	
	# create cur & idempotent
	cur = get_Redshift_connection()
	sql = "DELETE FROM jaeheon209.name_gender"
	cur.execute(sql)
	for r in lines[1:]:
		if r != '':
		(name, gender) = r.split(",")
		print(name, "-", gender)
		sql = "INSERT INTO jaeheon209.name_gender VALUES ('{name}', '{gender}')".format(name=name, gender=gender)
		print(sql)
		cur.execute(sql)

task = PythonOperator(
	task_id = 'perform_etl',
	python_callable = etl,
	dag = dag_second_assignment)
  
# task가 하나 밖에 없는 경우 아무 것도 하지 않아도 그냥 실행됨