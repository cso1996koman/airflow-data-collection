import logging
from airflow import DAG
from datetime import datetime, timedelta
from datetime import datetime, timedelta
from kosis_open_api_dag import KosisOpenApiDag
from data_collection_source_name_enum import DATACOLLECTIONSOURCENAME
from kosis_url import PRDSEENUM, KosisUrl
from kosis_open_api_dag_param_dto import OpenApiKosisDagParamDto
from weatheradministration_open_api_dag_param_dto import WeatherAdministrationOpenApiDagParamDto
from url_object_factory import UrlObjectFactory
from api_admin_dvo import ApiAdminDvo
from typing import List
import pytz
from weatheradministration_open_api_dag import WeatherAdministrationOpenApiDag
from weatheradministration_url import WeatherAdministrationUrl


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class OpenApiDagFactory:
    """
    This class represents a data collection DAG for OpenAPIs.
    Methods:
    - dag_factory(default_args: dict) -> List[DAG]: Generates a list of DAG objects based on the provided default arguments.
    - dag_generator(url: str, src_nm: str, dag_id: str, schedule_interval: timedelta, start_date: datetime, default_args: dict) -> DAG: Generates a DAG object based on the provided parameters.
    """
    
    @staticmethod
    def dag_factory(_default_args : dict, _kosis_api_admin_dvos : List[ApiAdminDvo], _weatheradministration_api_admin_dvos : List[ApiAdminDvo]) -> List[DAG]:
        kosis_urls : List[KosisUrl] = []
        weatheradministration_urls : List[WeatherAdministrationUrl] = []
        open_api_kosis_dag_param_dtos : List[OpenApiKosisDagParamDto] = []
        open_api_weatheradministration_dag_param_dtos : List[WeatherAdministrationOpenApiDagParamDto] = []
        for kosis_api_admin_dvo in _kosis_api_admin_dvos:
            open_api_kosis_dag_param_dto : OpenApiKosisDagParamDto = OpenApiKosisDagParamDto(src_nm = kosis_api_admin_dvo.src_nm,
                                                                                             tb_code = kosis_api_admin_dvo.tb_code,
                                                                                             tb_nm = kosis_api_admin_dvo.tb_nm,
                                                                                             uri = kosis_api_admin_dvo.uri,
                                                                                             dir_path = kosis_api_admin_dvo.dir_path,
                                                                                             api_keys = ["OTYwYjBlMGMyZmM2MmRlZDk0MjdjYWFhZWZmYTMwM2E="])            
            open_api_kosis_dag_param_dtos.append(open_api_kosis_dag_param_dto)
            kosis_urls.append(UrlObjectFactory.createKosisUrl(kosis_api_admin_dvo.uri))
        for weatheradministrataion_api_admin_dvo in _weatheradministration_api_admin_dvos:
            open_api_weatheradministration_dag_param_dto : WeatherAdministrationOpenApiDagParamDto = WeatherAdministrationOpenApiDagParamDto(src_nm = weatheradministrataion_api_admin_dvo.src_nm,
                                                                                                                                             tb_code = weatheradministrataion_api_admin_dvo.tb_code,
                                                                                                                                             tb_nm = weatheradministrataion_api_admin_dvo.tb_nm,
                                                                                                                                             uri = weatheradministrataion_api_admin_dvo.uri, 
                                                                                                                                             dir_path = weatheradministrataion_api_admin_dvo.dir_path, 
                                                                                                                                             service_keys = ["gAsNsUhyrbEmGPOt/eP8GO1Bf5ALh/akqttu0dJIpnR/q1LS2o+Ym0v8SDoMMTvAxNR8G1wNmB/xEWlf9CrSyg=="])
            open_api_weatheradministration_dag_param_dtos.append(open_api_weatheradministration_dag_param_dto)            
            weatheradministration_urls.append(UrlObjectFactory.createWeatherAdministrationUrl(weatheradministrataion_api_admin_dvo.uri))        
        Dags : List[DAG] = []
        for index, kosis_url in enumerate(kosis_urls):
            open_api_kosis_dag_param_dto : OpenApiKosisDagParamDto = open_api_kosis_dag_param_dtos[index]
            dag_id = f'{open_api_kosis_dag_param_dto.remove_except_alphanumericcharacter_dashe_dot_underscore(open_api_kosis_dag_param_dto.tb_code)}_{open_api_kosis_dag_param_dto.remove_except_alphanumericcharacter_dashe_dot_underscore(open_api_kosis_dag_param_dto.src_nm)}_{open_api_kosis_dag_param_dto.remove_except_alphanumericcharacter_dashe_dot_underscore(open_api_kosis_dag_param_dto.tb_nm)}'        
            schedule_interval :timedelta = timedelta(days=1)
            if(kosis_url.prdSe == PRDSEENUM.YEAR):
                schedule_interval = timedelta(days=365)
            elif(kosis_url.prdSe == PRDSEENUM.MONTH):
                schedule_interval = timedelta(days=30)
            elif(kosis_url.prdSe == PRDSEENUM.QUARTER):
                schedule_interval = timedelta(days=120)            
            start_date = datetime(2015, 1, 1, tzinfo=pytz.UTC)            
            dag_function_obj = OpenApiDagFactory.dag_generator(open_api_kosis_dag_param_dto.to_dict(),
                                                               dag_id,
                                                               schedule_interval,
                                                               start_date,
                                                               _default_args)
            Dags.append(dag_function_obj)
        for open_api_weatheradministration_dag_param_dto in open_api_weatheradministration_dag_param_dtos:
            dag_id = f'{open_api_weatheradministration_dag_param_dto.remove_except_alphanumericcharacter_dashe_dot_underscore(open_api_weatheradministration_dag_param_dto.tb_code)}_{open_api_weatheradministration_dag_param_dto.remove_except_alphanumericcharacter_dashe_dot_underscore(open_api_weatheradministration_dag_param_dto.src_nm)}_{open_api_weatheradministration_dag_param_dto.remove_except_alphanumericcharacter_dashe_dot_underscore(open_api_weatheradministration_dag_param_dto.tb_nm)}'
            schedule_interval = timedelta(days=1)
            start_date = datetime(2015, 1, 1, tzinfo=pytz.UTC)              
            dag_function_obj = OpenApiDagFactory.dag_generator(open_api_weatheradministration_dag_param_dto.to_dict(),
                                                               dag_id,
                                                               schedule_interval,
                                                               start_date,
                                                               _default_args)
            Dags.append(dag_function_obj)
        return Dags                 
    
    @staticmethod    
    def dag_generator(dag_config_param : dict,
                      dag_id : str,
                      schedule_interval : timedelta,
                      start_date : datetime,
                      default_args : dict) -> DAG:
        new_dag : DAG = None
        if(dag_config_param['src_nm'] == DATACOLLECTIONSOURCENAME.KOSIS.value):
            new_dag = KosisOpenApiDag.create_kosis_open_api_dag(dag_config_param, dag_id, schedule_interval, start_date, default_args)
        elif(dag_config_param['src_nm'] == DATACOLLECTIONSOURCENAME.WEATHERADMINISTRATION.value):
            new_dag = WeatherAdministrationOpenApiDag.create_weatheradministration_open_api_dag(dag_config_param, dag_id, schedule_interval, start_date, default_args)
        elif(dag_config_param['src_nm'] == DATACOLLECTIONSOURCENAME.PANDAS.value):
            pass
        elif(dag_config_param['src_nm'] == DATACOLLECTIONSOURCENAME.YFINANCE.value):
            pass
        elif(dag_config_param['src_nm'] == DATACOLLECTIONSOURCENAME.PUBLICDATAPORTAL.value):
            pass
        elif(dag_config_param['src_nm'] == DATACOLLECTIONSOURCENAME.LOGISTICSINFOCENTER.value):
            pass
        
        return new_dag
    

    

