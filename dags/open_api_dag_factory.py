from airflow import DAG
from datetime import datetime, timedelta
from datetime import datetime, timedelta
from weatheradministration_open_api_dag import WeatherAdministrationOpenApiDag
from kosis_open_api_dag import KosisOpenApiDag
from data_collection_source_name_enum import DATACOLLECTIONSOURCENAME
from kosis_url import PRDSEENUM, KosisUrl
from dag_param_dto import DagParamDto
from url_object_factory import UrlObjectFactory
from api_admin_dvo import ApiAdminDvo
from typing import List
class OpenApiDagFactory:
    @staticmethod
    def dag_factory(_default_args : dict, _api_admin_dvos : List[ApiAdminDvo]) -> List[DAG]:
        dag_list : List[DAG] = []
        for api_admin_dvo in _api_admin_dvos:
            dvo : ApiAdminDvo = api_admin_dvo
            if(dvo.src_nm == DATACOLLECTIONSOURCENAME.KOSIS.value):
                schedule_interval : timedelta = None
                start_date : datetime = None                
                dag_param_dto = DagParamDto(src_nm = dvo.src_nm,
                             tb_code = dvo.tb_code,
                             tb_nm = dvo.tb_nm,
                             uri = dvo.uri,
                             dir_path = dvo.dir_path,
                             api_keys = ["OTYwYjBlMGMyZmM2MmRlZDk0MjdjYWFhZWZmYTMwM2E="])
                kosis_url_obj : KosisUrl = UrlObjectFactory.createKosisUrl(dag_param_dto.uri)
                if kosis_url_obj.prdSe == PRDSEENUM.YEAR.value:
                    schedule_interval = timedelta(days=365)
                    assert(len(kosis_url_obj.startPrdDe) == 4), "InvalidPrdSe"
                    start_date_str : str = f"{kosis_url_obj.startPrdDe}-01-01"
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")                    
                elif kosis_url_obj.prdSe == PRDSEENUM.MONTH.value:
                    schedule_interval = timedelta(days=30)
                    assert(len(kosis_url_obj.startPrdDe) == 6), "InvalidPrdSe"
                    start_date_str : str = f"{kosis_url_obj.startPrdDe[0:4]}-{kosis_url_obj.startPrdDe[4:6]}-01"
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                elif kosis_url_obj.prdSe == PRDSEENUM.QUARTER.value:
                    schedule_interval = timedelta(days=90)
                    # 1분기 kosis_url_obj.startPrdDe : 20XX-01, 2분기 kosis_url_obj.startPrdDe : 20XX-02, 3분기 kosis_url_obj.startPrdDe : 20XX-03, 4분기 kosis_url_obj.startPrdDe : 20XX-04
                    assert(len(kosis_url_obj.startPrdDe) == 6), "InvalidPrdSe"
                    month : int = int(kosis_url_obj.startPrdDe[5:6])
                    if(month > 1):
                        month = (month - 1) * 3 + 1                    
                    start_date_str : str = f"{kosis_url_obj.startPrdDe[0:4]}-{month:02d}-01"
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")                    
                else:
                    assert False, "Invalid prdSe" 
                kosis_open_api_dag = KosisOpenApiDag.create_kosis_open_api_dag(dag_config_param=dag_param_dto.to_dict(),
                                                                                    dag_id=dag_param_dto.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dto.tb_code}_{dag_param_dto.src_nm}_{dag_param_dto.tb_nm}'),
                                                                                    schedule_interval=schedule_interval,
                                                                                    start_date=start_date,
                                                                                    default_args=_default_args)
                dag_list.append(kosis_open_api_dag)
            elif(dvo.src_nm == DATACOLLECTIONSOURCENAME.WEATHERADMINISTRATION.value):
                dag_param_dto = DagParamDto(src_nm = dvo.src_nm,
                             tb_code = dvo.tb_code,
                             tb_nm = dvo.tb_nm,
                             uri = dvo.uri,
                             dir_path = dvo.dir_path,
                             api_keys = ["%2BODpMm%2FIQ2XvsE4H4adLL5A5Oc7bExWMxoT1AlGn8Up%2BAzzvEQ4zxh7WhZK6Z278Of4pxFE%2Bp4Zh7XqZHTFctA%3D%3D"])
                weatheradministration_open_api_dag : WeatherAdministrationOpenApiDag = WeatherAdministrationOpenApiDag.create_weatheradministration_open_api_dag(dag_config_param=dag_param_dto.to_dict(),
                                                                                                                                                                 dag_id = dag_param_dto.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dto.tb_code}_{dag_param_dto.src_nm}_{dag_param_dto.tb_nm}'),
                                                                                                                                                                 schedule_interval=timedelta(days=1),
                                                                                                                                                                 start_date=datetime(2015, 1, 1),
                                                                                                                                                                 default_args=_default_args)
                dag_list.append(weatheradministration_open_api_dag)
            elif(dvo.src_nm == DATACOLLECTIONSOURCENAME.LOGISTICSINFOCENTER.value):
                pass
            elif(dvo.src_nm == DATACOLLECTIONSOURCENAME.PANDAS.value):
                pass
            elif(dvo.src_nm == DATACOLLECTIONSOURCENAME.PUBLICDATAPORTAL.value):   
                pass
            elif(dvo.src_nm == DATACOLLECTIONSOURCENAME.LOGISTICSINFOCENTER.value):
                pass
            else:
                assert False, "Invalid src_nm"                            
        return dag_list        
