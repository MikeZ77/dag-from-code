from typing import Callable

from engine.task import Task

# TODO: Execution context should be shared between processes so that a user can ...
#       set() and get() between tasks.

# from multiprocessing import Manager
# manager = Manager()
# Manager.register('ExecutionContext', ExecutionContext)
# execution_context = manager.ExecutionContext()
# Process(target=worker1, args=(execution_context,)) 

# Another interesting idea could be a method, wait_for_item('a') that blocks until 'a' is ...
# added to the store (since the order of execution is not guaranteed).
# Alternatively you could add e.g.wait_for=[task_2] in the task definition.

class ExecutionContext:
    def __init__(self):
        self.flow_fn: Callable[..., None] = None
        self.tasks: dict[str, Task] = {}
        # TODO: Allow the user to store their own values
        self.store = None
        # TODO: Load env variables
        # TODO: Create flow run with a unique id
        # TODO: Create a logger
    
    def register_flow(self, fn: Callable):
        # print("Registering: ", fn.__name__)
        self.flow_fn = fn
    
    def register_task(self, task: Task):
        self.tasks[task.task_name] = task
    
    # E.g. a logger that the client can use and that automatically sends logs to the server.
    def logger():
        ...
        
        
execution_context = ExecutionContext()