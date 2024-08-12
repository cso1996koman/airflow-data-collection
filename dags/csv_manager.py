import csv
from typing import Dict

class CsvManager:
    
    def save_csv(self, json_data: Dict, csv_path: str):
        """
        JSON 데이터를 받아 CSV 파일로 저장합니다.
        
        :param json_data: JSON 데이터 (Dict 형태)
        :param csv_path: 저장할 CSV 파일 경로
        """
        # JSON 데이터를 CSV로 변환하여 저장
        try:
            with open(csv_path, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                
                # JSON 데이터의 키를 CSV 헤더로 사용
                header = json_data[0].keys()
                writer.writerow(header)
                
                # JSON 데이터의 값을 CSV 행으로 사용
                for item in json_data:
                    writer.writerow(item.values())
                    
            print(f"CSV file saved successfully at {csv_path}")
        except Exception as e:
            print(f"Failed to save CSV file: {e}")
            raise
        