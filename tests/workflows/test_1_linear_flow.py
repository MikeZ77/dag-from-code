from engine.engine import flow, task

def step_1():
    return "a"

def step_2(a):
    assert a == "a"
    return "b"

def step_3(b):
    assert b == "b"
    return "c"

def step_4(c):
    assert c == "c"
    
def workflow():
    a = step_1()
    b = step_2(a)
    c = step_3(b)
    step_4(c)
    
def test_linear_workflow():
    task(step_1)
    task(step_2)
    task(step_3)
    task(step_4)
    flow(workflow)()
