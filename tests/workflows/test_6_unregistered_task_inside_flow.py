from engine.engine import flow, task

def not_a_task(b):
    print("not_a_task")
    return b

@task  
def task_1():
    print("task_1")
    return 1

@task
def task_2(b):
    print("task_2")
    
    
@flow
def workflow():
    a = task_1()
    b = not_a_task(a)
    task_2(b)

def test_unregistered_task_inside_flow():
    workflow()