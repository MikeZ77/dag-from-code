# TODO: It would be neat to read task comments and types and display these in a UI graph (if a UI is ever built)
# TODO: Maybe we can "inject" deps into the task i.e. create some kind of simple built in DI framework
# TODO: A task should be able to retry
# TODO: A task should have a "STATE" handler: e.g. could call another function
# @task(deps=[])
# @task should register this function as a task
def task_1(input):
    print("Task_1")
    return input + 1

def task_2(a):
    print("Task_2")
    return 1, 2

def task_3(input):
    print("Task_3")

def task_4(b, c):
    print(b, c)

def task_5():
    print("I do nothing")

def workflow(input: int):
    a = task_1(input)
    b, c = task_2(a)
    task_3(input)
    task_4(b, c)
    task_5()
    
    
if __name__ == "__main__":
    workflow(2)