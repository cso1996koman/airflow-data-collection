import re


class WeatherAdministrationOpenApiDagParamDto:
    def __init__(self, src_nm : str, tb_code : str, tb_nm : str, uri : str, dir_path : str, service_keys : str):
        self.src_nm : str = src_nm 
        self.tb_code : str = tb_code
        self.tb_nm : str = tb_nm
        self.uri : str = uri
        self.dir_path : str = dir_path
        self.service_keys : str = service_keys
    def to_dict(self):
        return {
            "src_nm": self.src_nm,
            "tb_code": self.tb_code,
            "tb_nm": self.tb_nm,
            "uri": self.uri,            
            "dir_path": self.dir_path,
            "service_keys": self.service_keys
        }
    def remove_except_alphanumericcharacter_dashe_dot_underscore(self, param_str : str) -> str:
        return re.sub(r'[^a-zA-Z0-9-_\.]', '', param_str)
        
    