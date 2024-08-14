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
            def open_api_request():
                context = get_current_context()
                prev_task_instance : TaskInstance = None
                cur_task_instance : TaskInstance = None
                request_url : str = None
                if context['task_instance'].get_previous_ti() is None:
                    prev_task_instance = context['task_instance']
                    cur_task_instance = context['task_instance']
                    request_url = dag_config_param['uri']
                    kosis_url_obj : KosisUrl = UrlObjectFactory.createKosisUrl(request_url)
                    if kosis_url_obj.prdSe == PRDSEENUM.YEAR.value:
                        kosis_url_obj.startPrdDe = start_date.strftime('%Y')
                        kosis_url_obj.endPrdDe = (start_date + timedelta(days=365)).strftime('%Y')
                        assert kosis_url_obj.startPrdDe is not "2015", "init start date year must be 2015"
                        assert kosis_url_obj.endPrdDe is not "2016", "init end date year must be 2016"
                    elif kosis_url_obj.prdSe == PRDSEENUM.MONTH.value:
                        kosis_url_obj.startPrdDe = start_date.strftime('%Y%m')
                        kosis_url_obj.endPrdDe = (start_date + timedelta(days=30)).strftime('%Y%m')
                        assert kosis_url_obj.startPrdDe is not "201501", "init start date year,month must be 201501"
                        assert kosis_url_obj.endPrdDe is not "201502", "init end date year,month must be 201502"
                    elif kosis_url_obj.prdSe == PRDSEENUM.QUARTER.value:
                        kosis_url_obj.startPrdDe = start_date.strftime('%Y%m')
                        kosis_url_obj.endPrdDe = (start_date + timedelta(days=120)).strftime('%Y%m')
                        assert kosis_url_obj.startPrdDe is not "201501", "init start date year,month must be 201501"
                        assert kosis_url_obj.endPrdDe is not "201504", "init end date year,month must be 201504"
                    else: 
                        assert False, "prdSe is not valid"
                    request_url = kosis_url_obj.get_full_url()
                else:
                    prev_task_instance = context['task_instance'].get_previous_ti()
                    cur_task_instance = context['task_instance']                    
                    assert prev_task_instance is not None, "prev_task_instance is None"                    
                    pre_task_instance_xcom = prev_task_instance.xcom_pull(key ='open_api_request')
                    assert pre_task_instance_xcom is not None, "pre_task_instance_xcom is None"
                    prev_task_instance_xcom_dto : KosisOpenApiRequestTaskXcomDto = KosisOpenApiRequestTaskXcomDto.from_dict(pre_task_instance_xcom)
                    request_url = prev_task_instance_xcom_dto.request_url
                
                url_obj : KosisUrl = UrlObjectFactory.createKosisUrl(request_url)
                url_obj.apiKey = dag_config_param['api_keys']
                open_api_helper_obj = OpenApiHelper()
                if(url_obj.objL1 != "All" and url_obj.objL1 != ""):
                    # objL1 : 1+2+3+4+5+...n+
                    # objL1 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL1)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL2 != "All" and url_obj.objL2 != ""):
                    # objL2 = 1+2+3+4+5+...n+
                    # objL2 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL2)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL3 != "All" and url_obj.objL3 != ""):
                    # objL3 = 1+2+3+4+5+...n+
                    # objL3 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL3)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL4 != "All" and url_obj.objL4 != ""):
                    # objL4 = 1+2+3+4+5+...n+
                    # objL4 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL4)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL5 != "All" and url_obj.objL5 != ""):
                    # objL5 = 1+2+3+4+5+...n+
                    # objL5 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL5)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL6 != "All" and url_obj.objL6 != ""):
                    # objL6 = 1+2+3+4+5+...n+
                    # objL6 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL6)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL7 != "All" and url_obj.objL7 != ""):
                    # objL7 = 1+2+3+4+5+...n+
                    # objL7 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL7)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                elif(url_obj.objL8 != "All" and url_obj.objL8 != ""):
                    # objL8 = 1+2+3+4+5+...n+
                    # objL8 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(url_obj.objL8)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(url_obj, obj_unit_params)
                else:
                    response = open_api_helper_obj.get_response(url_obj.get_full_url())
                cur_task_instance_xcom_dto = KosisOpenApiRequestTaskXcomDto(response_json=response, request_url=url_obj.get_full_url())                                
                prdSe = url_obj.prdSe
                if prdSe == PRDSEENUM.YEAR.value:
                    start_prd_de = datetime.strptime(url_obj.startPrdDe, '%Y').replace(tzinfo=pytz.UTC)
                    start_prd_de_year = start_prd_de.year
                    end_prd_de = datetime.strptime(url_obj.endPrdDe, '%Y').replace(tzinfo=pytz.UTC)
                    end_prd_de_year = end_prd_de.year
                    if start_prd_de_year + 1 >= end_prd_de_year:
                        end_prd_de += relativedelta(years=1)
                    else:
                        start_prd_de += relativedelta(years=1)
                    url_obj.startPrdDe = start_prd_de.strftime('%Y')
                    url_obj.endPrdDe = end_prd_de.strftime('%Y')                
                elif prdSe == PRDSEENUM.MONTH.value:
                    start_prd_de = datetime.strptime(url_obj.startPrdDe, '%Y%m').replace(tzinfo=pytz.UTC)
                    start_prd_de_month = start_prd_de.month
                    end_prd_de = datetime.strptime(url_obj.endPrdDe, '%Y%m').replace(tzinfo=pytz.UTC)
                    end_prd_de_month = end_prd_de.month
                    if start_prd_de_month + 1 >= end_prd_de_month:
                        end_prd_de += relativedelta(months=1)
                    else:
                        start_prd_de += relativedelta(months=1)
                    url_obj.startPrdDe = start_prd_de.strftime('%Y%m')
                    url_obj.endPrdDe = end_prd_de.strftime('%Y%m')                
                elif prdSe == PRDSEENUM.QUARTER.value:
                    start_prd_de = datetime.strptime(url_obj.startPrdDe, '%Y%m').replace(tzinfo=pytz.UTC)
                    start_prd_de_month = start_prd_de.month
                    end_prd_de = datetime.strptime(url_obj.endPrdDe, '%Y%m').replace(tzinfo=pytz.UTC)
                    end_prd_de_month = end_prd_de.month
                    if start_prd_de_month + 3 >= end_prd_de_month:
                        end_prd_de += relativedelta(months=3)
                    else:
                        start_prd_de += relativedelta(months=3)
                    url_obj.startPrdDe = start_prd_de.strftime('%Y%m')
                    url_obj.endPrdDe = end_prd_de.strftime('%Y%m')
                else:
                    assert False, "prdSe is not valid"
                cur_task_instance_xcom_dto.request_url = url_obj.get_full_url()
                cur_task_instance.xcom_push(key='open_api_request', value=cur_task_instance_xcom_dto.to_dict())
            @task
            def openapi_csv_save():            
                cur_task_instance = get_current_context()['task_instance']
                assert cur_task_instance is not None
                cur_task_instance_xcom : dict = cur_task_instance.xcom_pull(key='open_api_request')
                assert cur_task_instance_xcom is not None
                cur_task_instance_xcom_dto : KosisOpenApiRequestTaskXcomDto = KosisOpenApiRequestTaskXcomDto.from_dict(cur_task_instance_xcom)
                csv_manager : CsvManager = CsvManager()         
                dag_dir_path : str = dag_config_param['dir_path'].replace('TIMESTAMP', datetime.now().strftime('%Y%m%d'))        
                csv_manager.save_csv(json_data = cur_task_instance_xcom_dto.response_json, csv_path = dag_dir_path)
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
            
            open_api_request_task = open_api_request()
            openapi_csv_save_task = openapi_csv_save()
            openapi_upload_to_hdfs_task = openapi_upload_to_hdfs()
                    
            open_api_request_task >> openapi_csv_save_task >> openapi_upload_to_hdfs_task
        return kosis_open_api_dag()