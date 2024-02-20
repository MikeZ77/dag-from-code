from functools import reduce
from operator import mul

from engine.engine import flow, task

@task
def get_data():
    return [1, 2, 3, 4, 5, 6, 7, 8]

task_mapper = task(map)
task_reducer = task(reduce)

@task
def show_data(data):
    print(data)

@flow
def workflow():
    data = get_data()
    mapped_data = task_mapper(lambda x: x**2, data)
    reduced_data = task_reducer(mul, mapped_data)
    show_data(reduced_data)
    
def test_imported_function_as_task():
    workflow()



