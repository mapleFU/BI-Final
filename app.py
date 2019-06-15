from flask import Flask, jsonify
from neo4j import GraphDatabase
from py2neo import Graph, NodeMatcher, Database
import py2neo

import os

driver_address = ''

exist = os.environ.get('is_local', None)
if exist is None:
    driver_address = 'bolt://0.tcp.ngrok.io:19185'
else:
    driver_address = "bolt://localhost:7687"

g = Graph(driver_address)


app = Flask(__name__)


def permlize_result(record: py2neo.Record):
    for k, v in dict(record).items():
        print(type(v.labels))
        value_dict = dict(v)
        value_dict["id"] = value_dict['permID']
        return value_dict


def merge_result(result: py2neo.Cursor):
    return [permlize_result(r) for r in result]


@app.route('/')
def hello_world():
    cql = "MATCH (p:Person)-[t:TenureInOrg]->(o:Organization) RETURN t LIMIT 20"
    return jsonify(merge_result(g.run(cql)))


@app.route('/nmsl')
def load_all():
    cql = '''
        MATCH (p:Person)-[]-(f:Officership)-[]-(o:Organization)
        return distinct o LIMIT 5
    '''

    return jsonify(merge_result(g.run(cql)))


if __name__ == '__main__':
    app.run(debug=True)
