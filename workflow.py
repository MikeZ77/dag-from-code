from engine.engine import flow, task

# TODO: It would be neat to read task comments and types and display these in a UI graph (if a UI is ever built)
# TODO: Maybe we can "inject" deps into the task i.e. create some kind of simple built in DI framework
# TODO: For more fine grained control, it would be good if a task could be run on the ThreadPool or ProcessPool
# TODO: A task should have a "STATE" handler: e.g. could call another function
# TODO: A task can also be a class. This would allow more complicated behavior and state ...
#       must implement a run() method so the task runner can execute it.
# @task(deps=[])

# from functools import reduce

# class SomeClass:
#     ...

# def not_a_task(b):
#     print("not_a_task")
#     return b

# @task  
# def task_1():
#     print("task_1")
#     return 1

# @task
# def task_2(b):
#     print("task_2")
    
    
# @flow
# def workflow():
#     my_var, abc = "x", "y"
#     xyz, d = [SomeClass(), reduce()]
#     tt = reduce
#     a = task_1()
#     b = not_a_task(a)
#     task_2(b)

# if __name__ == "__main__":
#     workflow()

@task
def step_1():
    print("Input", None)
    return "a"

@task
def step_2(a):
    print("Input", a)
    return "b"

@task
def step_3(b):
    print("Input", b)
    return "c"

@task
def step_4(c):
    print("Input", c)
    print(c)
    
@flow()
def workflow():
    a = step_1()
    b = step_2(a)
    c = step_3(b)
    step_4(c)

if __name__ == "__main__":
    workflow()
