from dataclasses import dataclass

@dataclass
class WeatherAdministrataionOpenApiTaskXcomDto:
    response_json : dict
    csv_file_path : str
    hdfs_file_path : str
    