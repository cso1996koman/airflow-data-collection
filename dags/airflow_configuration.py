from datetime import datetime, timedelta
import logging
from typing import List
from api_admin_dao import ApiAdminDao
from api_admin_dvo import ApiAdminDvo
from open_api_dag_factory import OpenApiDagFactory
from airflow import DAG
from airflow.models import Variable
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
}

api_admin_dao : ApiAdminDao = ApiAdminDao('load_admin_db_mariadb')
api_admin_dvos_bykosis : List[ApiAdminDvo] = api_admin_dao.selectBySrcNm('kosis')
api_admin_dvos_byweatheradministration : List[ApiAdminDvo] = api_admin_dao.selectBySrcNm('기상청')
Variable.set("kosis_api_key", "OTYwYjBlMGMyZmM2MmRlZDk0MjdjYWFhZWZmYTMwM2E=")
Variable.set("weather_administration_api_key", "gAsNsUhyrbEmGPOt/eP8GO1Bf5ALh/akqttu0dJIpnR/q1LS2o+Ym0v8SDoMMTvAxNR8G1wNmB/xEWlf9CrSyg==")

# Call the dag_factory method to get a list of DAGs
dags : List[DAG] = OpenApiDagFactory.dag_factory(default_args, api_admin_dvos_bykosis, api_admin_dvos_byweatheradministration)

# Log the type of each item in the returned list
for dag in dags:
    logging.info(f"Type of dag: {type(dag)}")

# Register each DAG in the dags list
for dag in dags:
    dag()
    