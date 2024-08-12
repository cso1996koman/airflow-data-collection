from dataclasses import dataclass, field
from typing import Dict, Optional

@dataclass
class KosisOpenApiRequestTaskXcomDto:
    request_rul: Optional[str] = field(default=None)
    reponse_json: Optional[Dict] = field(default=None)
    csv_file_path: Optional[str] = field(default=None)
    hdfs_file_path: Optional[str] = field(default=None)

    def __init__(self, request_rul: Optional[str] = None, reponse_json: Optional[Dict] = None, 
                 csv_file_path: Optional[str] = None, hdfs_file_path: Optional[str] = None):
        self.request_rul = request_rul
        self.reponse_json = reponse_json
        self.csv_file_path = csv_file_path
        self.hdfs_file_path = hdfs_file_path

    def to_dict(self):
        return {
            "request_rul": self.request_rul,
            "reponse_json": self.reponse_json,
            "csv_file_path": self.csv_file_path,
            "hdfs_file_path": self.hdfs_file_path
        }
    def from_dict(self, dict):
        return KosisOpenApiRequestTaskXcomDto(request_rul = dict['request_rul'],
                                              reponse_json = dict['reponse_json'],
                                              csv_file_path = dict['csv_file_path'],
                                              hdfs_file_path = dict['hdfs_file_path'])
        
        
    
        
    