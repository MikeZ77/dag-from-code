from engine.engine import flow, task

def task_1(input):
    return input + 1

def task_2(a):
    assert a == 3
    return 1, 2

def task_3(input):
    assert input == 2

def task_4(b, c):
    assert b == 1 and c == 2

def task_5():
    ...

def workflow(input: int = 2):
    a = task_1(input)
    b, c = task_2(a)
    task_3(input)
    task_4(b, c)
    task_5()
    
    
def test_banching_workflow():
    task(task_1)
    task(task_2)
    task(task_3)
    task(task_4)
    task(task_5)
    flow(workflow)(2)