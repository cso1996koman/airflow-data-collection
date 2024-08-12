from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow import DAG
from open_api_helper import OpenApiHelper
from url_object_factory import UrlObjectFactory
from weatheradministration_open_api_dag_param_dto import WeatherAdministrationOpenApiDagParamDto
from weatheradministration_open_api_task_xcom_dto import WeatherAdministrataionOpenApiTaskXcomDto

class WeatherAdministrationOpenApiDag:
    @staticmethod
    def create_weatheradministration_open_api_dag(dag_config_param : dict, dag_id : str, schedule_interval : timedelta, start_date : datetime, default_args : dict) -> DAG:
        @dag(dag_id=dag_id,
                    schedule_interval=schedule_interval,
                    params=dag_config_param,
                    start_date=start_date,
                    default_args=default_args)
        def weatheradministration_open_api_dag():
            @task
            def open_api_request():
                weatheradministration_open_api_dag_param : WeatherAdministrationOpenApiDagParamDto = WeatherAdministrationOpenApiDagParamDto.from_dict(dag_config_param)
                weatheradministratation_url_obj = UrlObjectFactory.createWeatherAdministrationUrl(weatheradministration_open_api_dag_param.uri) 
                weatheradministratation_url_obj.serviceKey = weatheradministration_open_api_dag_param.service_keys
                open_api_helper = OpenApiHelper()
                response = open_api_helper.get_response(weatheradministratation_url_obj)
                return WeatherAdministrataionOpenApiTaskXcomDto(reponse_json=response)
            @task
            def open_api_csv_save(weatheradministration_open_api_task_xcom_dto : WeatherAdministrataionOpenApiTaskXcomDto):
                response_json = weatheradministration_open_api_task_xcom_dto.reponse_json
                csv_file_path = f"/tmp/{dag_id}.csv"
                with open(csv_file_path, 'w') as f:
                    f.write(response_json)
                weatheradministration_open_api_task_xcom_dto.csv_file_path = csv_file_path
                return weatheradministration_open_api_task_xcom_dto            
            @task
            def open_api_hdfs_save(weatheradministration_open_api_task_xcom_dto : WeatherAdministrataionOpenApiTaskXcomDto):
                csv_file_path = weatheradministration_open_api_task_xcom_dto.csv_file_path
                hdfs_file_path = f"{dag_id}.csv"
                webhdfs_hook = WebHDFSHook(webhdfs_conn_id='webhdfs_default')
                webhdfs_hook.load_file(csv_file_path, hdfs_file_path)
                weatheradministration_open_api_task_xcom_dto.hdfs_file_path = hdfs_file_path
                return weatheradministration_open_api_task_xcom_dto            
            open_api_request>>open_api_csv_save>>open_api_hdfs_save                
            return weatheradministration_open_api_dag
        
        
        