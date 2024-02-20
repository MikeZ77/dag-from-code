from engine.engine import flow, task

@task
def regestered_task_not_used():
    print("task_not_registered")
    
    
@task
def registered_task_is_used():
    print("registered_task_is_used")
    
    
@flow()
def workflow():
    registered_task_is_used()
    
def test_registered_tasks_not_called():
    workflow()