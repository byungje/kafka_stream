from kafka import KafkaConsumer
from json import loads #읽어오는 함수
import time
import datetime
import pyTigerGraph as tg

# tigergraphDB 연결
hostname = "http://52.79.164.131/"
graphName = "recommend_movie"
ID = "tigergraph"
PW = "1120"
conn = tg.TigerGraphConnection(host=hostname, graphname=graphName, username=ID, password=PW)


# kafka consumer 연결
Server = '52.79.164.131:9092'

topic_name = "byungje_movie"
consumer=KafkaConsumer(topic_name, #읽어올 토픽의 이름 필요.
                        bootstrap_servers=[Server], # 어떤 서버에서 읽어 올지 지정
                        auto_offset_reset="earliest", # 어디서부터 값을 읽어올지 (earliest 가장 처음 latest는 가장 최근)
                        enable_auto_commit=True, # 완료되었을 떄 문자 전송
                        #group_id='my-group', # 그룹핑하여 토픽 지정할 수 있다 > 같은 컨슈머로 작업
                        value_deserializer=lambda x: loads(x.decode('utf-8')), # 역직렬화 ( 받을 떄 ) ; 메모리에서 읽어오므로 loads라는 함수를 이용한다. // 직렬화 (보낼 떄)
                        consumer_timeout_ms=1000 # 입력한 ms초  이후에 메시지가 오지 않으면 없는 것으로 취급.
                    )
print("Start Insert data to Movie vertex")

for mes in consumer:
    # time.sleep(3)
    each_data1 = mes.value.items()
    #print(each_data)
    #print(list(each_data)[0])
    #results = conn.upsertVertices("SNOMEDCT_Concept", each_data)
    each_data = list(each_data1)
    results = conn.upsertVertex(each_data[0][1]["vertex_type"], each_data[0][1]["id"], each_data[0][1]["attribute"])
    # insert data
    # print(results)
    print(mes)

topic_name1 = "byungje_rating"
consumer=KafkaConsumer(topic_name1, #읽어올 토픽의 이름 필요.
                        bootstrap_servers=[Server], # 어떤 서버에서 읽어 올지 지정
                        auto_offset_reset="earliest", # 어디서부터 값을 읽어올지 (earliest 가장 처음 latest는 가장 최근)
                        enable_auto_commit=True, # 완료되었을 떄 문자 전송
                        #group_id='my-group', # 그룹핑하여 토픽 지정할 수 있다 > 같은 컨슈머로 작업
                        value_deserializer=lambda x: loads(x.decode('utf-8')), # 역직렬화 ( 받을 떄 ) ; 메모리에서 읽어오므로 loads라는 함수를 이용한다. // 직렬화 (보낼 떄)
                        consumer_timeout_ms=1000 # 입력한 ms초  이후에 메시지가 오지 않으면 없는 것으로 취급.
            )
print("Start Insert data to rate edge")
for mes in consumer:
    # time.sleep(3)
    each_data1 = mes.value.items()
    #print(each_data)
    #print(list(each_data)[0])
    #results = conn.upsertVertices("SNOMEDCT_Concept", each_data)
    each_data = list(each_data1)
    results = conn.upsertEdge(each_data[0][1]["source_vertex_type"], each_data[0][1]["source_id"], each_data[0][1]["edge_type"], each_data[0][1]["target_vertex_type"], each_data[0][1]["target_id"], each_data[0][1]["attributes"])
    # rating이 문자가 아닌 숫자라서 문제 발생 문자로 변경
    # print(results)
