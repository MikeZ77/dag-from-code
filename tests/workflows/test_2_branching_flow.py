from engine.engine import flow, task

@task
def task_1(input):
    print("Task_1", input)
    return input + 1

@task
def task_2(a):
    print("Task_2", a)
    return 1, 2

@task
def task_3(input):
    print("Task_3", input)

@task
def task_4(b, c):
    print("Task_4", b, c)

@task
def task_5():
    print("I do nothing")

@flow()
def workflow(input: int = 2):
    a = task_1(input)
    b, c = task_2(a)
    task_3(input)
    task_4(b, c)
    task_5()
    
    
def test_banching_workflow():
    workflow(2)