import requests
from typing import List, Dict

class OpenApiHelper:
    
    def __init__(self):
        """
        초기화 메서드로, 필요한 초기 설정을 수행합니다.
        """
        pass

    def get_multi_unit_param(self, unit_param: str) -> List[str]:
        """
        주어진 단위 파라미터를 '+'로 분리하여 리스트로 반환합니다.
        """
        return [param + '+' for param in unit_param.split('+')]

    def get_appeneded_response_bymulti_unit_param(self, url_obj, unit_params: List[str]) -> Dict:
        """
        여러 단위 파라미터를 사용하여 API 요청을 보내고, 그 응답을 합쳐서 반환합니다.
        """
        combined_response = {}
        for param in unit_params:
            # URL 객체의 단위 파라미터를 현재 단위 파라미터로 설정
            url_obj.unit_param = param
            # API 요청 보내기
            response = requests.get(url_obj.get_full_url())
            if response.status_code == 200:
                data = response.json()
                # 응답 데이터를 합치기
                combined_response.update(data)
            else:
                # 오류 처리
                print(f"Error fetching data for param {param}: {response.status_code}")
        return combined_response
        
    def get_response(self, url_obj) -> Dict:
        """
        단일 API 요청을 보내고, 그 응답을 반환합니다.
        """
        response = requests.get(url_obj.get_full_url())
        if response.status_code == 200:
            return response.json()
        else:
            # 오류 처리
            print(f"Error fetching data: {response.status_code}")
            return {}
    
    def assert_valid_unit_param(self, unit_param: str):
        """
        주어진 단위 파라미터가 '1+2+3+...n+' 형태인지 확인합니다.
        """
        parts = unit_param.split('+')
        for part in parts:
            if part and not part.isdigit():
                raise AssertionError(f"Invalid unit param: {unit_param}")