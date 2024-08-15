import csv
import os
from typing import Dict, List, Union

class CsvManager:
    
    def save_csv(self, json_data: Union[Dict, List[Dict]], csv_path: str):
        """
        JSON 데이터를 받아 CSV 파일로 저장합니다.
        
        :param json_data: JSON 데이터 (Dict 또는 List[Dict] 형태)
        :param csv_path: 저장할 CSV 파일 경로
        """
        # 디렉토리 경로 추출
        directory = os.path.dirname(csv_path)
        
        # 디렉토리가 존재하지 않으면 생성
        if not os.path.exists(directory):
            os.makedirs(directory, mode=0o755, exist_ok=True)
        
        # JSON 데이터를 CSV로 변환하여 저장
        try:
            with open(csv_path, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                
                # json_data가 리스트인지 확인
                if isinstance(json_data, list) and json_data:
                    # JSON 데이터의 키를 CSV 헤더로 사용
                    header = json_data[0].keys()
                    writer.writerow(header)
                    
                    # JSON 데이터의 값을 CSV 행으로 사용
                    for item in json_data:
                        writer.writerow(item.values())
                elif isinstance(json_data, dict):
                    # JSON 데이터의 키를 CSV 헤더로 사용
                    header = json_data.keys()
                    writer.writerow(header)
                    
                    # JSON 데이터의 값을 CSV 행으로 사용
                    writer.writerow(json_data.values())
                else:
                    raise ValueError("json_data must be a non-empty list of dictionaries or a dictionary")
                    
            print(f"CSV file saved successfully at {csv_path}")
        except Exception as e:
            print(f"Failed to save CSV file: {e}")
            raise