from common import load_graph
from py2neo.data import Node, Relationship
from py2neo import Graph, NodeMatcher, Database

import time
import os


def bench_cql(graph: Graph, cql: str):
    begin_time = time.time()
    for _ in range(2000):
        graph.run(cql)

    end_time = time.time()
    print(f"Begin {begin_time}, end {end_time}, cost {end_time - begin_time}")


if __name__ == '__main__':

    g = load_graph()

    bench_cql(g, "match(o:ns6__Organization{ns0__hasPermId:'4296405163'})<--(r)--"
                 "(p:ns8__Person{ns0__hasPermId:'34418264994'})return o,r,p")

    bench_cql(g, "EXPLAIN match(o:Organization :NewResource {permID:'4296405163'})<-[r]-"
                 "(p:Person :NewResource {permID:'34418264994'}) return o,r,p")

    # bench_cql(g, """
    # match(o:Organization :NewResource {permID:'4296405163'})<-[r]-(p:Person :NewResource {permID:'34418264994'})
    # USING INDEX p:NewResource(permID)
    # RETURN o, r, p
    # """)
    # cnt = 0
    # for result in g.run("MATCH (p: Person) return p"):
    #     cnt += 1
    #     if cnt % 1000 == 0:
    #         print(f" count up to {cnt}")
    # print("Done")

