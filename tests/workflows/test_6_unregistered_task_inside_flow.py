import pytest

from engine.engine import flow, task
from engine.exceptions import UnregisteredTaskCalled

def not_a_task(b):
    return b

def task_1():
    return 1

def task_2(b):
    assert b == 1
    
def workflow():
    a = task_1()
    b = not_a_task(a)
    task_2(b)

def test_unregistered_task_inside_flow():
    task(task_1)
    task(task_2)
    workflow_ = flow(workflow)
    with pytest.raises(UnregisteredTaskCalled):
        workflow_()