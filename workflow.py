# from engine.engine import flow, task

# TODO: It would be neat to read task comments and types and display these in a UI graph (if a UI is ever built)
# TODO: Maybe we can "inject" deps into the task i.e. create some kind of simple built in DI framework
# TODO: For more fine grained control, it would be good if a task could be run on the ThreadPool or ProcessPool
# TODO: A task should have a "STATE" handler: e.g. could call another function
# TODO: A task can also be a class. This would allow more complicated behavior and state ...
#       must implement a run() method so the task runner can execute it.
# @task(deps=[])

from engine.engine import flow, task


def task_decrement_value(input):
    print(input)
    return input - 1

def task_multiply_value(value):
    print(value)
    assert value == 9
    return value * 10
    
def task_divide_value(value):
    print(value)
    assert value == 90
    return value // 9

def final_value(value):
    print(value)
    assert value == 10

# def creates_a():
#     return "a"

# def does_nothing(a):
#     print("Does nothing")


def workflow(input):
    value = task_decrement_value(input)
    value = task_multiply_value(value)
    value = task_divide_value(value)
    final_value(value)
    
if __name__ == "__main__":
    task(task_decrement_value)
    task(task_multiply_value)
    task(task_divide_value)
    task(final_value)
    flow(workflow)(10)


    
