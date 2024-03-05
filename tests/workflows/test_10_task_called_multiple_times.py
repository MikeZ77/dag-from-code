from engine.engine import flow, task


def task_with_same_name_2(value):
    assert value == 3
    
def task_with_same_name_1(value):
    return value + 1


def workflow(input):
    first_output = task_with_same_name_1(input)
    second_output = task_with_same_name_1(first_output)
    task_with_same_name_2(second_output)
    task_with_same_name_2(second_output)
    
def test_task_called_multiple_times():
    task(task_with_same_name_1)
    task(task_with_same_name_2)
    flow(workflow)(1)
    