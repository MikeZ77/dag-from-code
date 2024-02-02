from __future__ import annotations

# TODO: Automate the number of processes created by checking for bottlenecks (max number of siblings) using BFS
# TODO: Allow the engine to use either processes or threads based on user preference
# TODO: The "Graph" is really the "Flow". However, this may be too much functionality. It may make sense to ...
#       create the graph seperately and inject it into the Flow from engine.py ...
#       maybe TaskDict should actually be the graph and perform the graph like functions

import multiprocessing as mp

from typing import Callable
from collections import UserDict 
from graphviz import Digraph
from collections.abc import Iterable, Iterator
from multiprocessing import Process, Queue
from queue import Queue as Queue_
from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class Edge:
    head: str
    tail: str   

# TODO: There needs to be Task and Flow STATES
# The state objects get returned through the message
@dataclass
class Message:
    task: Task

# @dataclass
# class TaskEndMessage:
#     pid: int
#     task: Task

# @dataclass
# class TaskStartMessage:
#     pid: int
#     task: Task

# TODO: Also make this a context manager
# Have it create the processes and Queue
class ProcessPool(Iterable):
    def __init__(self, pool_size: int):
        self.pool_size = pool_size
        self.processes: list[Process] = []
        self.busy: list[bool] = []
        self.end_queue: Queue[Message] = None
        self.process_queues: dict[str, Queue[Message]] = {}
    
    def create_process_pool(self):
        self.end_queue = Queue()
        
        for _ in range(self.pool_size):
            process_queue = Queue()
            process = Process(target=Graph.wait_for_task, args=(process_queue, self.end_queue))
            self.process_queues[process.name] = process_queue
            self.processes.append(process)

        self.busy = [False] * len(self.processes)

        
    def __iter__(self):
        return ProcessPoolIterator(self.processes, self.busy)
    
    def __enter__(self):
        self.create_process_pool()
        
        for process in self.processes:
            process.start()
            
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        for process in self.processes:
            process.join()
        
        for queue in self.process_queues.values():
            queue.close()
            queue.join_thread() # Find out why/if join thread is needed

        self.end_queue.close()
        self.end_queue.join_thread()


class ProcessPoolIterator(Iterator):
    def __init__(self, processes, busy):
        self.processes = processes
        self.busy = busy
        self.item = 0
        
    def __iter__(self):
        return self
    
    def __next__(self) -> Process | None:
        while self.busy[self.item]:
            self.item += 1
        
        if self.item >= len(self.processes):
            raise StopIteration
            
        return self.processes[self.item]
    

# TODO: This should probably be a collections.abc instead of a dict override.
#       That way we can provide iterators for traversing the graph in different ways ...
#       and create methods like get_task(), get_children(), etc. 
class TaskDict(UserDict):
    def __init__(self, data: dict[Task, set], **kwargs):
        # maps task_name to Task which is the key for data
        self.task_map = {task.task_name: task for task in data.keys()}
        super().__init__(data, **kwargs)
    
    def __getitem__(self, key):
        if isinstance(key, str):
            return self.task_map[key]
        return self.data[key]
        

class Task:
    def __init__(self, name: str, fn: Callable):
        self.task_name = name
        self.fn = fn 
        # TODO: we need to differentiate between *args and **kwargs
        # {"variable_name_1": payload, "variable_name_2": payload ...} 
        self.inputs = {}
        self.outputs: dict | None = None
        # We need to return a payload with the name of the variables. So the output of fn can be mapped to self.outputs.
        self.output_variables = [] # [variable_name_1, variable_name_2]
        
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.task_name == other.task_name
        return False

    def __hash__(self):
        return hash(self.task_name)
    
    def run(self):
        ...
    
class Graph:
    """A flow has a Graph representation of Tasks as nodes"""
    def __init__(self, task_fns, flow_name: str, cpu_cores = None, flow_input = dict()):
        # TODO: it should not be possible to create a cycle ...
        #       if the same task is called multiple times give it a sequence number e.g. my_task-0 ... my_task-n
        self.task_fns = dict(task_fns)
        self.flow_name = flow_name
        self.flow_input = flow_input
        self.graph: dict[Task, set[Task]] = TaskDict({Task(name, fn): set() for name, fn in task_fns})
        self.waiting_tasks = Queue_()
        self.pool_size = mp.cpu_count() if not cpu_cores else cpu_cores
        
    def add_edge(self, variables: list[str], edge: Edge):
        head, tail = edge.head, edge.tail
        head_task: Task = self.graph[head]
        tail_task: Task = self.graph[tail]
        
        self.graph[head_task].add(tail_task)
        tail_task.inputs = {var: None for var in variables}
        head_task.output_variables.extend(variables)
        

    def draw(self):
        graph = Digraph(format='pdf')
        for head in self.graph:
            for tail in self.graph[head]:
                graph.edge(head.task_name, tail.task_name)
            
            if not self.graph[head]:
                graph.node(head.task_name)
        
        graph.render(self.flow_name, cleanup=True)

    def set_flow_input(self):
        flow_task: Task = self.graph[self.flow_name]
        for task in self.graph[flow_task]:
            for variable, _ in task.inputs.items():
                if variable in self.flow_input:
                    task.inputs[variable] = self.flow_input[variable]
     
    def get_entry_tasks(self) -> set[Task]:
        return set(task for task in self.graph.keys() if not task.inputs and task.task_name != self.flow_name)
    

    def pass_outputs_to_next_task(self, task: Task):
        for next_task in self.graph[task]:
            next_task.inputs = dict(next_task.inputs & next_task.outputs)
                
    def is_task_ready(self, task: Task, pool: ProcessPool) -> bool:
        # TODO: Technically user could return None in a variable and this would not work ...
        #       I think the plan is to return an object that contains STATE and data.
        return all(input for input in task.inputs.values())
    
    def submit_next_task(self, tasks: set[Task], pool: ProcessPool):
        # TODO: If process is None (i.e. there is no waiting process) add this to a waiting_tasks queue.
        processes = iter(pool)    
        for next_task in tasks:
            if self.is_task_ready(next_task):
                process = next(processes, None)
                queue = pool.process_queues[process.name]
                queue.put(Message(next_task))
        
    def flow_run_complete(self) -> bool:
        ...
        
    @staticmethod
    def wait_for_task(process_queue: Queue[Message], end_queue: Queue[Message]):
        while message := process_queue.get():
            task = message.task
            inputs, output_variables, fn = task.inputs, task.output_variables, task.fn
            # TODO: Note inputs assumes only *args and not **kwargs currently
            output = fn(*inputs)

            # TODO: Create a setter for task.outputs instead of having this here
            if isinstance(output, tuple):
                task.outputs = dict(zip(output, output_variables))
            elif output:
                [output_variables] = output_variables
                task.outputs = {output_variables: output}
                
            end_queue.put(Message(task))

    
    def run(self):
        # TODO: A Task is run once all its inputs have been recieved
        # 1. message_queue.get() returns message: {pid: 1234, task_name: "task_one", output: {out_one: 123, out_two: "abc"}} (dataclass) 
        # 2. Set the busy flag in the process_pool for that process to False
        # 2. then iterate through "task_one" 's children:
        #   a. for the task, check if it has the corresponding input and set it.
        #   b. if all inputs are set, then the task is ready to be run 
        #   c. Set the busy flag in the process_pool for that process to True
        
        # ...
        with ProcessPool(self.pool_size) as pool:
            # Set task input for tasks that are tail nodes to the flow input
            self.set_flow_input()
            
            # Get entrypoint tasks that have in-deg = 0
            entry_tasks = self.get_entry_tasks()
            
            # Start the entry point tasks
            self.submit_next_task(entry_tasks, pool)
       
            count = 0
            while message := pool.end_queue.get():
                # TODO: Every time we get a completed task, a process is free'd up ...
                # check if there are any waiting_tasks first and run it.
                # For testing
                print(message)
                count += 1
                
                if count > 0:
                    break
                
                task = message.task
                                                
                self.pass_outputs_to_next_task(task)
                self.submit_next_task(self.graph[task])
                
                if self.flow_run_complete():
                    ...
        

