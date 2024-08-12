from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.operators.python import get_current_context
from csv_manager import CsvManager
from kosis_open_api_task_xcom_dto import KosisOpenApiRequestTaskXcomDto
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.operators.python import get_current_context
from kosis_open_api_task_xcom_dto import KosisOpenApiRequestTaskXcomDto
from airflow import DAG
import logging
from typing import List
from dateutil.relativedelta import relativedelta
import pytz
from url_object_factory import UrlObjectFactory
from kosis_url import KosisUrl, PRDSEENUM
from kosis_open_api_task_xcom_dto import KosisOpenApiRequestTaskXcomDto
from open_api_helper import OpenApiHelper

class KosisOpenApiDag:
    
    @staticmethod
    def create_kosis_open_api_dag(dag_config_param : dict, dag_id : str, schedule_interval : timedelta, start_date : datetime, default_args : dict) -> DAG:
        @dag(dag_id=dag_id,
                    schedule_interval=schedule_interval,
                    params=dag_config_param,
                    start_date=start_date,
                    default_args=default_args)
        def kosis_open_api_dag() -> DAG:
            @task
            def open_api_request() -> KosisOpenApiRequestTaskXcomDto:
                context = get_current_context()
                prev_task_instance : TaskInstance = None
                cur_task_instance : TaskInstance = None
                request_url : str = None
                if context['task_instance'].previous_ti is None:
                    prev_task_instance = context['task_instance']
                    cur_task_instance = context['task_instance']
                    request_url = dag_config_param['uri']    
                else:
                    prev_task_instance = context['task_instance'].previous_ti                                       
                    cur_task_instance = context['task_instance']
                    prev_task_instance_xcom_dto : KosisOpenApiRequestTaskXcomDto = KosisOpenApiRequestTaskXcomDto.from_dict(prev_task_instance.xcom_pull(task_ids='open_api_request'))
                    request_url = prev_task_instance_xcom_dto.request_rul
                

                url_obj : KosisUrl = UrlObjectFactory.createKosisUrl(request_url)                
                open_api_helper_obj = OpenApiHelper()
                if(url_obj.objL1 != "All"):
                    # objL1 : 1+2+3+4+5+...n+
                    # objL1 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    open_api_helper_obj.assert_valid_unit_param(url_obj.objL1)
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL1)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL2 != "All"):
                    # objL2 = 1+2+3+4+5+...n+
                    # objL2 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    open_api_helper_obj.assert_valid_unit_param(url_obj.objL2)
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL2)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL3 != "All"):
                    # objL3 = 1+2+3+4+5+...n+
                    # objL3 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    open_api_helper_obj.assert_valid_unit_param(url_obj.objL3)
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL3)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL4 != "All"):
                    # objL4 = 1+2+3+4+5+...n+
                    # objL4 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    open_api_helper_obj.assert_valid_unit_param(url_obj.objL4)
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL4)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL5 != "All"):
                    # objL5 = 1+2+3+4+5+...n+
                    # objL5 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    open_api_helper_obj.assert_valid_unit_param(url_obj.objL5)
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL5)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL6 != "All"):
                    # objL6 = 1+2+3+4+5+...n+
                    # objL6 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    open_api_helper_obj.assert_valid_unit_param(url_obj.objL6)
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL6)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL7 != "All"):
                    # objL7 = 1+2+3+4+5+...n+
                    # objL7 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    open_api_helper_obj.assert_valid_unit_param(url_obj.objL7)
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL7)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL8 != "All"):
                    # objL8 = 1+2+3+4+5+...n+
                    # objL8 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    open_api_helper_obj.assert_valid_unit_param(url_obj.objL8)
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL8)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                else:
                    response = open_api_helper_obj.get_response(url_obj)
                cur_task_instance_xcom_dto = KosisOpenApiRequestTaskXcomDto(response_json = response)                                
                                
                prdSe = url_obj.prdSe
                if prdSe == PRDSEENUM.YEAR:
                    start_prd_de = datetime.strptime(url_obj.startPrdDe, '%Y').replace(tzinfo=pytz.UTC)
                    end_prd_de = datetime.strptime(url_obj.endPrdDe, '%Y').replace(tzinfo=pytz.UTC)
                    if start_prd_de >= end_prd_de:
                        end_prd_de += relativedelta(years=1)
                    else:
                        start_prd_de += relativedelta(years=1)
                    url_obj.startPrdDe = start_prd_de.strftime('%Y')
                    url_obj.endPrdDe = end_prd_de.strftime('%Y')
                
                elif prdSe == PRDSEENUM.MONTH:
                    start_prd_de = datetime.strptime(url_obj.startPrdDe, '%Y%m').replace(tzinfo=pytz.UTC)
                    end_prd_de = datetime.strptime(url_obj.endPrdDe, '%Y%m').replace(tzinfo=pytz.UTC)
                    if start_prd_de >= end_prd_de:
                        end_prd_de += relativedelta(months=1)
                    else:
                        start_prd_de += relativedelta(months=1)
                    url_obj.startPrdDe = start_prd_de.strftime('%Y%m')
                    url_obj.endPrdDe = end_prd_de.strftime('%Y%m')
                
                elif prdSe == PRDSEENUM.QUARTER:
                    start_prd_de = datetime.strptime(url_obj.startPrdDe, '%Y%m').replace(tzinfo=pytz.UTC)
                    end_prd_de = datetime.strptime(url_obj.endPrdDe, '%Y%m').replace(tzinfo=pytz.UTC)
                    if start_prd_de >= end_prd_de:
                        end_prd_de += relativedelta(months=3)
                    else:
                        start_prd_de += relativedelta(months=3)
                    url_obj.startPrdDe = start_prd_de.strftime('%Y%m')
                    url_obj.endPrdDe = end_prd_de.strftime('%Y%m')
                cur_task_instance_xcom_dto.request_rul = url_obj.getFullUrl()
                cur_task_instance.xcom_push(key='open_api_request', value=cur_task_instance_xcom_dto.to_dict())
            @task
            def openapi_csv_save():
                context = get_current_context()
                kosis_open_api_task_xcom_dto : KosisOpenApiRequestTaskXcomDto = context['task_instance'].xcom_pull(task_ids='open_api_request')
                csv_manager : CsvManager = CsvManager()                
                csv_manager.save_csv(json = kosis_open_api_task_xcom_dto.reponse_json, csv_path = '/tmp/response.csv')
            @task
            def openapi_upload_to_hdfs():
                file_path = '/tmp/response.csv'
                try:
                    hdfs_hook = WebHDFSHook(webhdfs_conn_id='local_hdfs')
                    hdfs_client = hdfs_hook.get_conn()
                    hdfs_client.upload('/test/data/example_data.csv', file_path)
                    logging.info("File uploaded to HDFS successfully")
                    # os.remove(file_path)
                except Exception as e:
                    logging.error(f"Failed to upload file to HDFS: {e}")
                    raise
            
            openapi_request_task = open_api_request()
            openapi_csv_save_task = openapi_csv_save()
            openapi_upload_to_hdfs_task = openapi_upload_to_hdfs()
                    
            openapi_request_task >> openapi_csv_save_task >> openapi_upload_to_hdfs_task
        return kosis_open_api_dag