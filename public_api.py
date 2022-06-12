import requests
import json
import sqlite3

url = 'http://apis.data.go.kr/B552584/UlfptcaAlarmInqireSvc/getUlfptcaAlarmInfo'
params ={'serviceKey' : 'a2mlXqfaWFJ+QLfTIMtxKNt6o1gH+U8qEMUOaOt9VT4eUeF7awntA9jh1otYlzRx9qpXRqBR75/DxHLF1dzMSg==', 'returnType' : 'json', 'numOfRows' : '100', 'pageNo' : '1', 'year' : '2020', 'itemCode' : 'PM10' }

response = requests.get(url, params=params)
parsered_api = json.loads(response.text)

public_data = parsered_api['response']['body']['items']

dust_list=[]
for id ,value in enumerate(public_data): ## enumerate로 순서 및 데이터 반환
    value.pop('sn') ## key값이 sn인 데이터만 삭제
    value.setdefault("id", id+1) ## key값이 id인 value값을 딕셔너리에 지정
    dust_list.append(value)

## 정상출력되는지 Test ##
# print(dust_list[0])
# print(list(dust_list[1].values()))



conn = sqlite3.connect('dust_db.sqlite3')
cur = conn.cursor()

## table이 존재하면 초기화 진행 ##
cur.execute("DROP TABLE IF EXISTS dust_info;")


## 테이블 생성 ##
cur.execute("""CREATE TABLE dust_info(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    clearVal INTEGER,
                    districtName VARCHAR(32),
                    dateDate DATETIME,
                    issueVal INTEGER,
                    issueTime TIME,
                    clearDate DATETIME,
                    issueDate DATETIME,
                    moveName VARCHAR(32),
                    clearTime TIME,
                    issueGbn VARCHAR(32),
                    itemCode CHAR(4));""")



## 생성한 테이블에 데이터 적재 ##
for i in range(len(dust_list)):
    temp_data = list(dust_list[i].values())
    print(temp_data)
    # print(list(dust_list[i].values()))
    cur.execute(f"""INSERT INTO dust_info
                    VALUES ({temp_data[11]},{temp_data[0]}, '{temp_data[1]}', '{temp_data[2]}', {temp_data[3]}, '{temp_data[4]}', '{temp_data[5]}',
                    '{temp_data[6]}', '{temp_data[7]}', '{temp_data[8]}', '{temp_data[9]}','{temp_data[10]}')""")


## MYSQL에서는 MODIFY가 작동하는데, 생성한 id 컬럼의 순서를 변경
# cur.execute("""ALTER TABLE dust_info MODIFY COLUMN id INTEGER FIRST;""")
conn.commit()

