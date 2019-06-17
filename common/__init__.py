import os

from py2neo import Graph
from elasticsearch import Elasticsearch


def load_graph_gen():
    loaded = False
    graph: Graph = None

    def _real_load():
        nonlocal graph
        exist = os.environ.get('is_local', None)
        if exist is None:
            driver_address = 'bolt://0.tcp.ngrok.io:14330'
        else:
            driver_address = "bolt://localhost:7687"
        graph = Graph(driver_address)
        loaded = True

    def _real_load_graph()->Graph:
        if not loaded:
            _real_load()
        return graph

    return _real_load_graph


# the real function to load the graph
load_graph = load_graph_gen()


def load_elastic_search():
    exist = os.environ.get('is_local', None)
    if exist is None:
        es_address = 'localhost'
    else:
        es_address = "maplewish.cn"
    es = Elasticsearch([es_address], http_compress=True)
    return es


__all__ = ["load_graph", "load_elastic_search"]


if __name__ == '__main__':
    # g: Graph = load_graph()
    load_elastic_search()
