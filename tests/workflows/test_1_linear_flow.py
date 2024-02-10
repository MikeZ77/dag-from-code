from engine.engine import flow, task

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

def test_linear_workflow():
    workflow()
