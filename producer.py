from kafka import KafkaProducer
from json import dumps
import time
import json
import pandas as pd

server = "52.79.164.131:9092"
topic_name = "byungje_movie"
topic_name1 = "byungje_rating"

pr = KafkaProducer( #acks=0,
                    bootstrap_servers = [server]
                    # ,value_serializer=lambda x : dumps(x).encode("utf-8")
                )
# Producer가 Consumer에 전달 할 데이터
movies = pd.read_csv("/home/tigergraph/05_movie/movie-rec_data/movies.csv")
ratings = pd.read_csv("/home/tigergraph/05_movie/movie-rec_data/ratings.csv")

print("Start Insert data to Kafka")
for i in range(len(movies)):
    m1 = {}
    m1[i] = {"vertex_type" : "movie", "id" : str(movies["movieId"][i]), "attribute" : {"title": movies["title"][i], "genres" : movies["genres"][i]}}

    print(m1[i])
    time.sleep(1)
    m2 = json.dumps(m1).encode("utf-8")
    pr.send(topic_name, m2)
pr.flush()

print("Start Insert data to Kafka")
for i in range(len(ratings[:100000])):
    r1 = {}
    r1[i] = {"source_vertex_type" : "person", "source_id" : str(ratings["userId"][i]), "edge_type" : "rate", "target_vertex_type" : "movie", "target_id" : str(ratings["movieId"][i]),  "attributes" : {"rating": str(ratings["rating"][i]), "rated_at" : str(ratings["timestamp"][i])}}
    print(r1[i])
    time.sleep(1)
    r2 = json.dumps(r1).encode("utf-8")
    pr.send(topic_name1, r2)
pr.flush()