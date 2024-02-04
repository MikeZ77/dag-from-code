# This file servers as the entry point for the flow run

from context import execution_context as ctx
from task import Task
from graph import BuildGraph

def task(__fn):
    # TODO: This should create the Task but for now lets just register the task fn
    # TODO: Validate a task is not being called from within a task
    task = Task(__fn.__name__, __fn)
    ctx.register_task(task)


# Flow arguments can be setup here
def flow(*, name: str = None, cpu_cores: int = 4, draw: bool = False): 
    def execute(__fn=None, *args, **kwargs):
        nonlocal name
        
        if not name:
            name = __fn.__name__
        
        # For the purposes of creating the graph, a flow taks is also created
        ctx.register_flow(__fn)
        ctx.register_task(Task(__fn.__name__, __fn))
        # TODO: Read all imports/dependencies into context (I think) so the child process can load them.
        # NOTE: This would not need to be done with threading since threads share the same memory space.
        print()
        graph = BuildGraph.from_code()
        
        if draw:
            graph.draw(name)
        
        
        # NOTE: Placeholder, since the client does not actually run the code.
        # Later, it might be useful to have the client run some callback like all_complete or any_failed
        def empty_callable():
            ...
        
        return empty_callable
        
        
    return execute
