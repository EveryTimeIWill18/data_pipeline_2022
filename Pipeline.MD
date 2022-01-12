In data engineering, the data pipeline is the real power behind production-grade databases, the optimization of SQL tables, and data structures and algorithms.

In a data pipeline, each task receives input and then returns output used in the next task.

diagram of a pipeline

The task is an abstract concept that we use to define parts of a pipeline. There is no right, or wrong way, to write a task. The only requirement is to allow inputs and return outputs that the next task can use as an input.

def task(input):
    output = do_something(input)
    return output

Copy
In this lesson, we'll compare functional programming to object-oriented programming (classes, objects, and state) — Python allows you to switch between the two. To get the most out of this lesson, you'll need to be comfortable programming in Python, and you'll need a general understanding of data engineering.

Here are a few takeaways you can expect by the end of the lesson:

The differences between imperative and functional programming
How to write Python code in a functional style using map, reduce, and filter
How to link functional programming with data pipelines
Let's get started!