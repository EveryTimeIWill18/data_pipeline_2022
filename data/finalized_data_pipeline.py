"""
finalized_data_pipeline
~~~~~~~~~~~~~~~~~~~~~~~
"""
from typing import List, Dict, Any
from collections import deque
from pprint import pprint

class DAG(object):
    """DAG (Directed Acyclic Graph) class
    Parameters
    ----------
    graph_
    """
    def __init__(self):
        self.graph_ = {}
        self.degrees_: Dict[int:Any] = {}

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

    def in_degrees(self):
        for node in self.graph_:
            if node not in self.degrees_:
                self.degrees_[node] = 0
            for pointed in self.graph_[node]:
                if pointed not in self.degrees_:
                    self.degrees_[pointed] = 0
                self.degrees_[pointed] += 1

    def sort_dag(self):
        """Topological sort algorithm"""
        self.in_degrees()
        to_visit = deque()
        for node in self.graph_:
            if self.degrees_[node] == 0:
                to_visit.append(node)

        searched = []
        while to_visit:
            node = to_visit.popleft()
            for pointer in self.graph_[node]:
                self.degrees_[pointer] -= 1
                if self.degrees_[pointer] == 0:
                    to_visit.append(pointer)
            searched.append(node)
        return searched


class Pipeline(object):
    def __init__(self):
        self.tasks: DAG = DAG()
        self.completed_tasks: Dict[int:Any] = {}

    def task(self, depends_on=None):
        def inner(func: callable):
            self.tasks.add(func)
            if depends_on:
                self.tasks.add(depends_on, func)
            return func
        return inner

    def run(self):
        """Run the Data Pipeline"""
        scheduled = self.tasks.sort_dag()
        for task in scheduled:
            for node, values in self.tasks.graph_.items():
                if task in values:
                    self.completed_tasks[task] = task(self.completed_tasks[node])
                if task not in self.completed_tasks:
                    self.completed_tasks[task] =  task()
        return self.completed_tasks






def main():

    data_pipeline = Pipeline()

    @data_pipeline.task()
    def first():
        return 20

    @data_pipeline.task(depends_on=first)
    def second(x):
        return x * 2

    @data_pipeline.task(depends_on=second)
    def third(x):
        return x / 3

    # @data_pipeline.task(depends_on=second)
    # def fourth(x):
    #     return x // 4
    #
    # @data_pipeline.task(depends_on=third)
    # def fifth(x):
    #     return x * x

    pprint(data_pipeline.tasks.graph_)

    output = data_pipeline.run()


    # dag = DAG()
    # dag.add(1)
    # dag.add(1, 3)
    # dag.add(1, 4)
    # dag.add(3, 5)
    # dag.add(2, 6)
    # dag.add(3, 6)
    # dag.add(4, 7)
    # dag.add(5, 7)
    # dag.add(6, 7)
    # dag.add(8)
    #
    # print(dag.graph_)
    # dag.in_degrees()
    # print(dag.degrees_)
    # sorted_dag = dag.sort_dag()
    # print(sorted_dag)


if __name__ == '__main__':
    main()