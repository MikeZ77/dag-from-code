# This file servers as the entry point for the flow run

import math
import inspect
import multiprocessing as mp

from engine.context import execution_context as ctx
from engine.task import Task
from engine.task_runner import TaskRunner
from engine.pool import ProcessPool
from engine.flow import Flow
from engine.graph import BuildGraph


def task(__fn):
    # TODO: This should create the Task but for now lets just register the task fn
    # TODO: Validate a task is not being called from within a task

    task = Task(__fn.__name__, __fn)
    ctx.register_task(task)
    return __fn



def flow(__fn=None, *, name: str = None, cpu_cores: int = None, draw: bool = False): 
    def flow_decorator(__fn):
        def execute(*args, **kwargs):
            nonlocal name
            nonlocal cpu_cores
            
            # TODO: This does not find default values
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
            print(graph)
            if draw:
                graph.draw(name)
            
            print(ctx.tasks)
            # for task in graph._graph:
            #     print(task)
            
            runner = TaskRunner()
            process_pool = ProcessPool(runner, pool_size=cpu_cores)
            # NOTE: For now just assume kwargs are being passed in.
            flow = Flow(name, graph, process_pool, flow_input)
            flow.run()
            
        return execute 
    
    if __fn:
        return flow_decorator(__fn)
    else:
        return flow_decorator
        
    
