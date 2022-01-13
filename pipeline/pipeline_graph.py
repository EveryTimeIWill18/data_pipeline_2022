"""
pipeline_graph.py
~~~~~~~~~~~~~~~~~
Graph: The data structure is composed of vertices (nodes) and edges (branches).
Directed: Each edge of a vertex points only in one direction.
Acyclic: The graph does not have any cycles.


(1) --> (2)
|
v
(4) ->  (6)

graph = {1 : {Vertex : [Vertex.id(2), Vertex.id(4)]} }

{Vertex: [Vertex.id(2), Vertex.id(4)]}

in_degrees:

for k in keys:
    current = k  # node(1)
    degrees = 0
    for i in list(dict.keys())
        if i != k:
            if k in dict[i]:
                degrees += 1
    in_degrees[k] = degrees

"""
from typing import List, Dict
from collections import deque


class DAG(object):
    def __init__(self):
        self.graph_ = {}
        self.degrees_: Dict[int:int] = {}
        self.graph: Dict[int: Dict[Vertex: List[int]]] = {}

    def add(self, node, to=None):
        if not node in self.graph_:
            self.graph_[node] = []
        if to:
            if not to in self.graph_:
                # if to (i.e. node 2 is not in the graph, create it)
                # Create the node and set to an empty list
                self.graph_[to] = []
            # Find the node and append to, to node's list
            self.graph_[node].append(to)

    def add_vertex(self, id: int, data: int, connections=[]):
        if len(self.graph) == 0:
            # No Vertices added to graph yet
            v = Vertex(id, data, connections)
            self.graph[id] = {v: connections}

    def in_degrees(self):
        self.degrees = {}
        for node in self.graph:
            if node not in self.degrees:
                self.degrees[node] = 0
            for pointed in self.graph[node]:
                if pointed not in self.degrees:
                    self.degrees[pointed] = 0
                self.degrees[pointed] += 1

    def will_sort_dag(self):
        to_visit = deque()

    def sort_dag(self):
        to_visit = deque()
        for node in self.graph:
            pass




    def get_in_degrees(self):
        for k in self.graph_.keys():
            degrees = 0
            for i in list(self.graph_.keys()):
                if k != i:
                    if k in self.graph_[i]:
                        degrees += 1
            self.degrees_[k] = degrees


class Vertex():
    def __init__(self, id, data, connections=None):
        self.id: int = id
        self.data: int = data
        self.connections: List[int] = connections


def main():
    dag = DAG()
    dag.add(1)
    dag.add(1, 3)
    dag.add(1, 4)
    dag.add(3, 5)
    dag.add(2, 6)
    dag.add(3, 6)
    dag.add(4, 7)
    dag.add(5, 7)
    dag.add(6, 7)
    dag.add(8)
    print(dag.graph_)
    dag.get_in_degrees()
    print(dag.degrees_)
    dag.sort_dag()
    # graph.add_vertex(id=1, data=100)
    # g = list(graph.graph[1].keys())
    # print(g[0].connections)


if __name__ == '__main__':
    main()