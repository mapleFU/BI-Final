from common import load_graph

if __name__ == '__main__':

    cql = """
        MATCH (n:Person :NewResource)-[r]->(m:Organization :NewResource) 
        with n,m,type(r) as t, tail(collect(r)) as coll SKIP {} LIMIT 300000
        foreach(x in coll | delete x)
    """
    graph = load_graph()
    cnt = 600000
    while cnt <= 6300000:
        print(f"start run {cnt}")
        graph.run(cql.format(cnt))
        cnt += 300000

