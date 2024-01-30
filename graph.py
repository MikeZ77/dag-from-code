from __future__ import annotations

# TODO: Automate the number of processes created by checking for bottlenecks (max number of siblings) using BFS
# TODO: Allow the engine to use either processes or threads based on user preference
# TODO: The "Graph" is really the "Flow". However, this may be too much functionality. It may make sense to ...
#       create the graph seperately and inject it into the Flow from engine.py ...
#       maybe TaskDict should actually be the graph and perform the graph like functions

import networkx as nx
import matplotlib.pyplot as plt
import multiprocessing as mp

from typing import Callable
from collections import UserDict 
from collections.abc import Iterable, Iterator
from multiprocessing import Process, Queue
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
    def __init__(self, processes: list[Process]):
        self.processes = processes
        self.busy = [False] * len(processes)

    def __iter__(self):
        return ProcessPoolIterator(self.processes, self.busy)
    
    def start(self):
        for process in self.processes:
            process.start()
    
    def end(self):
       for process in self.processes:
            process.join()
        
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
        
    # # Stores two references to the same object so we can access using a Task object or task_name
    # def __setitem__(self, key, value):
    #     self.data[key] = value
    #     if isinstance(key, Task):
    #         self.data[key.task_name] = value

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
    def __init__(self, task_fns, flow_name: str, draw_mode = False, cpu_cores = None, flow_input = dict()):
        # TODO: it should not be possible to create a cycle ...
        # if the same task is called multiple times give it a sequence number e.g. my_task-0 ... my_task-n
        self.task_fns = dict(task_fns)
        self.flow_name = flow_name
        self.flow_input = flow_input
        self.graph: dict[Task, set[Task]] = TaskDict({Task(name, fn): set() for name, fn in task_fns})
        self.draw_mode = draw_mode
        self.graph_ = nx.DiGraph() if self.draw_mode else None # We only keep this for the purpose of drawing the graph
        self.pool_size = mp.cpu_count() if not cpu_cores else cpu_cores
        
    def add_edge(self, variables: list[str], edge: Edge):
        head, tail = edge.head, edge.tail
        head_task: Task = self.graph[head]
        tail_task: Task = self.graph[tail]
        
        self.graph[head_task].add(tail_task)
        tail_task.inputs = {var: None for var in variables}
        head_task.output_variables.extend(variables)
        
        if self.draw_mode:
            self.graph_.add_edge(head, tail)
            
    def draw(self):
        if self.draw_mode:
            position = nx.spring_layout(self.graph_)
            nx.draw(self.graph_, position)
            # Note: On WSL so not worth doing the intsalls to get this to plt.show()
            plt.savefig(self.flow_name)

    def set_flow_input(self):
        flow_task: Task = self.graph[self.flow_name]
        for task in self.graph[flow_task]:
            for variable, _ in task.inputs.items():
                if variable in self.flow_input:
                    task.inputs[variable] = self.flow_input[variable]
     
    def get_entry_tasks(self) -> list[Task]:
        return [task for task in self.graph.keys() if not task.inputs and task.task_name != self.flow_name]
    
    def create_process_pool(self) -> tuple[ProcessPool, Queue, dict[int, Queue]]:
        # TODO: Each proccess actually needs it own queue, since the is no option to peek the queue before dequeing using queue.get()
        end_queue = Queue()
        process_queues = {}
        processes_pool = []
        for _ in range(self.pool_size):
            process_queue = Queue()
            process = Process(target=self.wait_for_task, args=(process_queue, end_queue))
            process_queues[process.name] = process_queue
            processes_pool.append(process)
        
        return ProcessPool(processes_pool), end_queue, process_queues
        

    def check_task_has_all_inputs(self):
        ...

    def check_flow_run_complete(self) -> bool:
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
        try:
            # Create process pool
            processes_pool, end_queue, process_queues = self.create_process_pool()
            processes_pool.start()     
            
            # Set task input for tasks that are tail nodes to the flow input
            self.set_flow_input()

            # Get entrypoint tasks that have in-deg = 0
            entry_tasks = self.get_entry_tasks()
            
            processes = iter(processes_pool)
            for task in entry_tasks:
                process = next(processes, None)
                queue = process_queues[process.name]
                queue.put(Message(task))
                

            count = 0
            while message := end_queue.get():
                print(message)
                count += 1
                
                # For testing
                if count > 0:
                    break
                    
                if self.check_flow_run_complete():
                    ...
                    
        except Exception as e:
            print(e)
        finally:              
            # Cleanup
            # TODO: Add this to context manager
            for queue in process_queues.values():
                queue.close()
                queue.join_thread() # Find out why/if join thread is needed

            end_queue.close()
            end_queue.join_thread()
            processes_pool.end()
