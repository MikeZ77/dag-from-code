from functools import partial

from engine.engine import flow, task

def parent_task():
    return 1
    
def child_task_1(input):
    return input + 1
    
    
def child_task_2(input):
    return input + 2   


def child_task_3(input):
    return input + 3


def child_task_4(input):
    return input + 4


def child_task_5(input):
    return input + 5


def child_task_6(input):
    return input + 6


def aggregate_task(a, b, c, d, e, f):
    sum([a, b, c, d, e, f])


def workflow():
    output = parent_task()
    a = child_task_1(output)
    b = child_task_2(output)
    c = child_task_3(output)
    d = child_task_4(output)
    e = child_task_5(output)
    f = child_task_6(output)
    aggregate_task(a, b, c, d, e, f)
    
    
def test_more_tasks_than_processes():
    task(parent_task)
    task(child_task_1)
    task(child_task_2)
    task(child_task_3)
    task(child_task_4)
    task(child_task_5)
    task(child_task_6)
    task(aggregate_task)
    flow_ = partial(flow, cpu_cores=2)
    flow_(workflow)()

