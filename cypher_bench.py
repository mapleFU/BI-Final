from neo4j import GraphDatabase
from py2neo.data import Node, Relationship
from py2neo import Graph, NodeMatcher, Database

import time
import os


def bench_cql(graph: Graph, cql: str):
    begin_time = time.time()
    for _ in range(100):
        graph.run(cql)
    # time.sleep(5)
    end_time = time.time()
    print(f"Begin {begin_time}, end {end_time}, cost {end_time - begin_time}")


if __name__ == '__main__':

    exist = os.environ.get('is_local', None)
    if exist is None:
        driver_address = 'bolt://0.tcp.ngrok.io:19185'
    else:
        driver_address = "bolt://localhost:7687"

    g = Graph(driver_address)

    # bench_cql(g, "match(o:ns6__Organization{ns0__hasPermId:'4296405163'})<--(r)--"
    #              "(p:ns8__Person{ns0__hasPermId:'34418264994'})return o,r,p")

    bench_cql(g, "EXPLAIN match(o:Organization :NewResource {permID:'4296405163'})<-[r]-"
                 "(p:Person :NewResource {permID:'34418264994'}) return o,r,p")
    # cnt = 0
    # for result in g.run("MATCH (p: Person) return p"):
    #     cnt += 1
    #     if cnt % 1000 == 0:
    #         print(f" count up to {cnt}")
    # print("Done")

