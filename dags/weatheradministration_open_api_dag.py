from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow import DAG
from weatheradministration_url import WeatherAdministrationUrl
from open_api_helper import OpenApiHelper
from url_object_factory import UrlObjectFactory
from airflow.operators.python import get_current_context
from dag_param_dto import DagParamDto
from open_api_xcom_dto import OpenApiXcomDto
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
                context = get_current_context()
                cur_task_instance = context['task_instance']
                prev_task_instance = cur_task_instance.get_previous_ti()
                request_url = None
                if prev_task_instance is None:
                    request_url = dag_config_param['url']
                    assert request_url is not None
                    weatheradministration_url_ojb : WeatherAdministrationUrl = UrlObjectFactory.createWeatherAdministrationUrl(request_url)
                    weatheradministration_url_ojb.startDt = datetime(2015, 1, 1).strftime('%Y%m%d')
                    weatheradministration_url_ojb.endDt = weatheradministration_url_ojb.startDt + timedelta(days=999)
                    assert datetime.strptime(weatheradministration_url_ojb.endDt, "%Y%m%d") < datetime.now() - timedelta(days=1), "endDt should be less than yesterday"
                    request_url = weatheradministration_url_ojb.getFullUrl()
                else:
                    prev_task_instance_xcom_dict : dict = prev_task_instance.xcom_pull(key=f"{dag_id}_open_api_request_{prev_task_instance.run_id}")
                    assert prev_task_instance_xcom_dict is not None
                    prev_task_instance_xcom_dto = OpenApiXcomDto.from_dict(prev_task_instance_xcom_dict)  
                    request_url = prev_task_instance_xcom_dto.next_request_url
                response_json = OpenApiHelper.get_response(request_url)
                weatheradministration_url_obj = UrlObjectFactory.createWeatherAdministrationUrl(request_url)
                if((weatheradministration_url_ojb.endDt + timedelta(days=999)) < (datetime.now() - timedelta(days=1))):
                    weatheradministration_url_ojb.startDt = weatheradministration_url_ojb.endDt + timedelta(days=1)
                    weatheradministration_url_ojb.endDt = weatheradministration_url_ojb.startDt + timedelta(days=999)
                else:
                    remaining_days = (datetime.now() - timedelta(days=1)) - weatheradministration_url_ojb.endDt
                    weatheradministration_url_ojb.startDt = weatheradministration_url_ojb.endDt + timedelta(days=1)
                    weatheradministration_url_ojb.endDt = weatheradministration_url_ojb.startDt + remaining_days
                next_request_url = weatheradministration_url_ojb.getFullUrl()
                open_api_xcom_dto = OpenApiXcomDto(next_request_url = next_request_url, response_json = response_json)
                cur_task_instance.xcom_push(key=f"{dag_id}_open_api_request_{cur_task_instance.run_id}", value=open_api_xcom_dto.to_dict())
            @task
            def open_api_csv_save():
                context = get_current_context()
                cur_task_instance = context['task_instance']
                cur_dag_run = cur_task_instance.dag_run
                open_api_request_task_instance = cur_dag_run.get_task_instance(task_id = 'open_api_request')
                open_api_request_task_instance_xcom_dict : dict = open_api_request_task_instance.xcom_pull(key=f"{dag_id}_open_api_request_{open_api_request_task_instance.run_id}")
                assert open_api_request_task_instance_xcom_dict is not None
                open_api_request_task_instance_xcom_dto = OpenApiXcomDto.from_dict(open_api_request_task_instance_xcom_dict)
                response_json = open_api_request_task_instance_xcom_dto.response_json
                csv_file_path : str = dag_config_param['dir_path']
                csv_file_path = csv_file_path.replace('TIMESTAMP', datetime.now().strftime('%Y%m%d'))
                OpenApiHelper.save_csv(response_json, csv_file_path)
                open_api_request_task_instance_xcom_dto.csv_file_path = csv_file_path
                cur_task_instance.xcom_push(key=f"{dag_id}_open_api_csv_save_{cur_task_instance.run_id}", value=open_api_request_task_instance_xcom_dto.to_dict())
            @task
            def open_api_hdfs_save():
                context = get_current_context()
                cur_dag_run = context['dag_run']
                open_api_csv_save_task_instance = cur_dag_run.get_task_instance(task_id = 'open_api_csv_save')
                open_api_csv_save_task_instance_xcom_dict : dict = open_api_csv_save_task_instance.xcom_pull(key=f"{dag_id}_open_api_csv_save_{cur_dag_run.run_id}")
                open_api_csv_save_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(open_api_csv_save_task_instance_xcom_dict)                
                csv_file_path = open_api_csv_save_task_instance_xcom_dto.csv_file_path
                hdfs_file_path = csv_file_path
                webhdfs_hook = WebHDFSHook(webhdfs_conn_id='webhdfs_default')
                webhdfs_hook.load_file(csv_file_path, hdfs_file_path)                
            open_api_request>>open_api_csv_save>>open_api_hdfs_save
            return weatheradministration_open_api_dag()