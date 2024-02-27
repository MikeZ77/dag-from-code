from engine.engine import flow, task

def regestered_task_not_used():
    ...
    
    
def registered_task_is_used():
    ...
    
    
def workflow():
    registered_task_is_used()
    
def test_registered_tasks_not_called():
    task(regestered_task_not_used)
    task(registered_task_is_used)
    flow(workflow)()