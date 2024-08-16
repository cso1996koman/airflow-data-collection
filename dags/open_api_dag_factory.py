import logging
from airflow import DAG
from datetime import datetime, timedelta
from datetime import datetime, timedelta
from kosis_open_api_dag import KosisOpenApiDag
from data_collection_source_name_enum import DATACOLLECTIONSOURCENAME
from kosis_url import PRDSEENUM, KosisUrl
from dag_param_dto import DagParamDto
from open_api_xcom_dto import OpenApiXcomDto
from url_object_factory import UrlObjectFactory
from api_admin_dvo import ApiAdminDvo
from typing import List
import pytz
from weatheradministration_open_api_dag import WeatherAdministrationOpenApiDag
from weatheradministration_url import WeatherAdministrationUrl
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
class OpenApiDagFactory:
    @staticmethod
    def dag_factory(_default_args : dict, _api_admin_dvos : List[ApiAdminDvo]) -> List[DAG]:
        dag_list : List[DAG] = []
        weatheradministration_urls : List[WeatherAdministrationUrl] = []
        open_api_kosis_dag_param_dtos : List[DagParamDto] = []        
        open_api_weatheradministration_dag_param_dtos : List[OpenApiXcomDto] = []
        for api_admin_dvo in _api_admin_dvos:
            dvo : ApiAdminDvo = api_admin_dvo
            if(dvo.src_nm == DATACOLLECTIONSOURCENAME.KOSIS.value):
                dag_param_dto = DagParamDto(src_nm = dvo.src_nm,
                             tb_code = dvo.tb_code,
                             tb_nm = dvo.tb_nm,
                             uri = dvo.uri,
                             dir_path = dvo.dir_path,
                             api_keys = ["OTYwYjBlMGMyZmM2MmRlZDk0MjdjYWFhZWZmYTMwM2E="])
                if dag_param_dto.src_nm == DATACOLLECTIONSOURCENAME.KOSIS.value:
                    kosis_url_obj : KosisUrl = UrlObjectFactory.createKosisUrl(dag_param_dto.uri)
                    if kosis_url_obj.prdSe == PRDSEENUM.YEAR.value:
                        schedule_interval = timedelta(days=365)
                    elif kosis_url_obj.prdSe == PRDSEENUM.MONTH.value:
                        schedule_interval = timedelta(days=30)                        
                    elif kosis_url_obj.prdSe == PRDSEENUM.QUARTER.value:
                        schedule_interval = timedelta(days=120)
                    else:
                        assert False, "Invalid prdSe"
                    kosis_open_api_dag = KosisOpenApiDag.create_kosis_open_api_dag(dag_config_param=dag_param_dto.to_dict(),
                                                                                        dag_id=f'{dag_param_dto.remove_except_alphanumericcharacter_dashe_dot_underscore(dag_param_dto.tb_code)}_{dag_param_dto.remove_except_alphanumericcharacter_dashe_dot_underscore(dag_param_dto.src_nm)}_{dag_param_dto.remove_except_alphanumericcharacter_dashe_dot_underscore(dag_param_dto.tb_nm)}',
                                                                                        rschedule_interval=schedule_interval,
                                                                                        start_date=datetime(2015, 1, 1, tzinfo=pytz.UTC),
                                                                                        default_args=_default_args)
                    dag_list.append(kosis_open_api_dag)                                        
        
