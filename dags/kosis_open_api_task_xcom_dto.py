from dataclasses import dataclass, field
from typing import Dict, Optional

@dataclass
class KosisOpenApiRequestTaskXcomDto:
    request_url: Optional[str] = field(default=None)
    response_json: Optional[Dict] = field(default=None)
    csv_file_path: Optional[str] = field(default=None)
    hdfs_file_path: Optional[str] = field(default=None)

    def __init__(self, request_url: Optional[str] = None, response_json: Optional[Dict] = None, 
                 csv_file_path: Optional[str] = None, hdfs_file_path: Optional[str] = None):
        self.request_url = request_url
        self.response_json = response_json
        self.csv_file_path = csv_file_path
        self.hdfs_file_path = hdfs_file_path

    def to_dict(self):
        return {
            "request_url": self.request_url,
            "response_json": self.response_json,
            "csv_file_path": self.csv_file_path,
            "hdfs_file_path": self.hdfs_file_path
        }
    @staticmethod
    def from_dict(dict):
        return KosisOpenApiRequestTaskXcomDto(request_url = dict['request_url'],
                                              response_json = dict['response_json'],
                                              csv_file_path = dict['csv_file_path'],
                                              hdfs_file_path = dict['hdfs_file_path'])
        
        
    
        
    