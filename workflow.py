from engine import flow, task

# TODO: It would be neat to read task comments and types and display these in a UI graph (if a UI is ever built)
# TODO: Maybe we can "inject" deps into the task i.e. create some kind of simple built in DI framework
# TODO: A task should be able to retry
# TODO: For more fine grained control, it would be good if a task could be run on the ThreadPool or ProcessPool
# TODO: A task should have a "STATE" handler: e.g. could call another function
# @task(deps=[])
# @task should register this function as a task

@task
def task_1(input):
    print("Task_1")
    return input + 1

@task
def task_2(a):
    print("Task_2")
    return 1, 2

@task
def task_3(input):
    print("Task_3")

@task
def task_4(b, c):
    print(b, c)

@task
def task_5():
    print("I do nothing")

@flow()
def workflow(input: int):
    a = task_1(input)
    b, c = task_2(a)
    task_3(input)
    task_4(b, c)
    task_5()
    
    
if __name__ == "__main__":
    workflow(2)