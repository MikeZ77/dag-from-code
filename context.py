from typing import Callable

from task import Task

class ExecutionContext:
    def __init__(self):
        self.flow_fn: Callable[..., None] = None
        self.tasks: dict[str, Task] = {}
        # TODO: Allow the user to store their own values
        self.store = None
        # TODO: Load env variables
        # TODO: Create flow run with a unique id
        # TODO: Create a slogger
    
    def register_flow(self, fn: Callable):
        self.flow_fn = fn
    
    def register_task(self, task: Task):
        self.tasks[task.task_name] = task
    
    # E.g. a logger that the client can use and that automatically sends logs to the server.
    def logger():
        ...
        
        
execution_context = ExecutionContext()