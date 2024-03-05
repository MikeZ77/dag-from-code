from engine.engine import flow, task


def task_decrement_value(input):
    return input - 1

def task_multiply_value(input):
    assert input == 9
    return input * 100
    
def task_divide_value(value):
    assert value == 900
    return value // 9

def final_value(value):
    assert value == 100


def workflow(input):
    value = task_decrement_value(input)
    value = task_multiply_value(value)
    value = task_divide_value(value)
    final_value(value)
    
def test_task_called_multiple_times():
    task(task_decrement_value)
    task(task_multiply_value)
    task(task_divide_value)
    task(final_value)
    flow(workflow)(10)
    