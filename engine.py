# This file servers as the entry point for the flow run

import math
import inspect
import multiprocessing as mp

from context import execution_context as ctx
from task import Task
from task_runner import TaskRunner
from pool import ProcessPool
from flow import Flow
from graph import BuildGraph


def task(__fn):
    # TODO: This should create the Task but for now lets just register the task fn
    # TODO: Validate a task is not being called from within a task
    task = Task(__fn.__name__, __fn)
    ctx.register_task(task)
    return __fn


# Flow arguments can be setup here
def flow(*, name: str = None, cpu_cores: int = None, draw: bool = False): 
    def wrapper(__fn=None):
        def execute(*args, **kwargs):
            nonlocal name
            nonlocal cpu_cores
            
            fn_params = inspect.signature(__fn).parameters
            arg_input = {param: arg for param, arg in zip(fn_params, args)}
            flow_input = {**arg_input, **kwargs}
            
            if not cpu_cores:
                cpu_cores = math.ceil(mp.cpu_count() / 4) 
            
            if not name:
                name = __fn.__name__
            
            # For the purposes of creating the graph, a flow taks is also created
            ctx.register_flow(__fn)
            ctx.register_task(Task(__fn.__name__, __fn))
            # TODO: Read all imports/dependencies into context (I think) so the child process can load them.
            # NOTE: This would not need to be done with threading since threads share the same memory space.

            graph = BuildGraph.from_code()
            
            if draw:
                graph.draw(name)
            
            runner = TaskRunner()
            process_pool = ProcessPool(runner, pool_size=cpu_cores)
            # NOTE: For now just assume kwargs are being passed in.
            flow = Flow(name, graph, process_pool, flow_input)
            flow.run()
            
        return execute 
    return wrapper
