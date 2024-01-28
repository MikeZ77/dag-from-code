# TODO: It would be neat to read task comments and types and display these in a UI graph (if a UI is ever built)
# TODO: Maybe we can "inject" deps into the task i.e. create some kind of simple built in DI framework
# @task(deps=[])
# @task should register this function as a task
def task_1(input):
    print("Task_1")
    return input + 1

def task_2():
    print("Task_2")
    return 1, 2

def task_3():
    print("Task_3")

def task_4(b, c):
    print(b, c)

def workflow(input: int):
    a = task_1(input)
    b, c = task_2(a)
    task_3(input)
    task_4(b, c)
    
if __name__ == "__main__":
    workflow(0)