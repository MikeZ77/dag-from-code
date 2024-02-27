from functools import reduce, partial
from operator import mul

from engine.engine import flow, task

def get_data():
    return [1, 2, 3, 4, 5, 6, 7, 8]

# TODO: There are too many issues with making standard library functions a task.

# task_mapper = task(map)
# task_reducer = task(reduce)

# must be in global scope and lambda cannot be pickled 
# some functions like map return an interator/generator
square = partial(map, lambda x: x**2) 
reducer = partial(reduce, mul)

# need to partially apply which removes fn __name__ used by task
square.__name__ = "square"
reducer.__name__ = "reducer"

def show_data(data):
    print(data)

def workflow():
    data = get_data()
    mapped_data = square(data)
    reduced_data = reducer(mapped_data)
    show_data(reduced_data)
    
def test_imported_function_as_task():
    task(square)
    task(reducer)
    task(get_data)
    task(show_data)
    flow(workflow)()

