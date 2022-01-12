"""
pipeline.py
~~~~~~~~~~~
"""
import os
from typing import List, Dict
from functools import partial, wraps
from pprint import pprint

# def parse_log(log):
#     for line in log:
#         split_line = line.split()
#         remote_addr = split_line[0]
#         time_local = parse_time(split_line[3] + " " + split_line[4])
#         request_type = strip_quotes(split_line[5])
#         request_path = split_line[6]
#         status = split_line[8]
#         body_bytes_sent = split_line[9]
#         http_referrer = strip_quotes(split_line[10])
#         http_user_agent = strip_quotes(" ".join(split_line[11:]))
#         yield (
#             remote_addr, time_local, request_type, request_path,
#             status, body_bytes_sent, http_referrer, http_user_agent
#         )

# def build_csv(lines, header=None, file=None):
#     if header:
#         lines = itertools.chain([header], lines)
#     writer = csv.writer(file, delimiter=',')
#     writer.writerows(lines)
#     file.seek(0)
#     return file
#
# def count_unique_request(csv_file):
#     reader = csv.reader(csv_file)
#     header = next(reader)
#     idx = header.index('request_type')
#
#     uniques = {}
#     for line in reader:
#
#         if not uniques.get(line[idx]):
#             uniques[line[idx]] = 0
#         uniques[line[idx]] += 1
#     return ((k, v) for k,v in uniques.items())

# Run the static tasks.
# log = open('example_log.txt')
# parsed = parse_log(log)
# file = open('temporary.csv', 'r+')
# csv_file = build_csv(
#     parsed,
#     header=[
#         'ip', 'time_local', 'request_type',
#         'request_path', 'status', 'bytes_sent',
#         'http_referrer', 'http_user_agent'
#     ],
#     file=file
# )
# uniques = count_unique_request(csv_file)
# summarized_file = open('summarized.csv', 'r+')
# summarized_csv = build_csv(uniques, header=['request_type', 'count'], file=summarized_file)




class Pipeline(object):
    def __init__(self):
        self.tasks: List[int] = []

    def task(self, depends_on=None):
        idx = 0
        if depends_on:
            idx = self.tasks.index(depends_on) + 1
        def inner(f):
            self.tasks.insert(idx, f)
            return f
        return inner

    def run(self, input_):
        output = input_
        for task in self.tasks:
            output = task(output)
        return output

pipeline = Pipeline()
log_file = r"C:\Users\murpwil\Desktop\local_git\python_projects\data_pipeline_2022\data\example_log.txt"

@pipeline.task()
def open_file(file_: str):
    f = open(file_)
    return f
@pipeline.task(depends_on=open_file)
def read_file(f):
    for line in f.readlines():
        pprint(line)
    return f
@pipeline.task(depends_on=read_file)
def close_file(f):
    f.close()

pipeline.run(input_=log_file)


# log_file = r"C:\Users\murpwil\Desktop\local_git\python_projects\data_pipeline_2022\data\example_log.txt"
# logs = open(log_file)

def parse_logs(logs):
    return









# @pipeline.task()
# def first_task(x):
#     return x + 1
#
# @pipeline.task(depends_on=first_task)
# def second_task(x):
#     return x*2
#
# @pipeline.task(depends_on=second_task)
# def third_task(x):
#     return x + 4
#
# print(pipeline.tasks)
# print(pipeline.run(20))















#
# def add(a, b):
#     return a+b
















# def logger(func):
#     def inner(*args):
#         print(f"Calling function: {func.__name__}")
#         print(f"With args: {args}")
#         return func(*args)
#     return inner
#
# logged_add = logger(add)(1,2)
# print(logged_add)

# Using Decorators
# def logger(func):
#     @wraps(func)
#     def inner(*args, **kwargs):
#         print(f"Calling function: {func.__name__}")
#         print(f"With args: {args}")
#         return func(*args)
#     return inner
#
#
# @logger
# def add_(a, b):
#     return a + b
#
# print(add_(1,2))

# @logger
# def add(a, b):
#     return a + b
#
# # Wrapping `add()` with `@logger`:
# add(1, 2) -> logger(add)(1, 2)


# def add_any(*args):
#     def inner(*inner_args):
#         print(f'args = {args}', {type(args)})
#         return sum(args + inner_args)
#     return inner
#
# add_four = add_any(1,2,3,4)
# print(add_four(5,6,7))



