# bigkinds에서 가져오는 파일에 대해서 실행될 예정인 파일입니다
# url 받아서 url의 도메인 네임을 인식하고 자동으로 셀렉터 돌려서 데이터 스키마에 적합한 형태로 처리하는 형태로 구상했습니다

from util.logger import Logger
from util.database import get_engine
from util.elastic import