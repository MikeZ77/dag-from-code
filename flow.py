from __future__ import annotations

# TODO: Automate the number of processes created by checking for bottlenecks (max number of siblings) using BFS
# TODO: Allow the engine to use either processes or threads based on user preference
# TODO: The "Graph" is really the "Flow". However, this may be too much functionality. It may make sense to ...
#       create the graph seperately and inject it into the Flow from engine.py ...
#       maybe TaskDict should actually be the graph and perform the graph like functions

# import functools
import multiprocessing as mp

from typing import  Any
from graphviz import Digraph
from collections import deque
from collections.abc import Iterable, Iterator
from multiprocessing import Process, Queue
from dataclasses import dataclass

from task import Task, TaskState
from graph import TaskDict

@dataclass(eq=True, frozen=True)
class Edge:
    head: str
    tail: str   

@dataclass
class TaskData:
    state: TaskState
    result: Any
    message: str

@dataclass
class TaskEndMessage:
    process_name: str
    task: Task

@dataclass
class TaskStartMessage:
    task: Task
    
@dataclass
class ProcessEndMessage:
    ...

# TODO: Also make this a context manager
# Have it create the processes and Queue
class ProcessPool(Iterable):
    def __init__(self, pool_size: int):
        self.pool_size = pool_size
        self.processes: list[Process] = []
        self.busy: list[bool] = []
        self.end_queue: Queue[TaskEndMessage] = None
        self.process_queues: dict[str, Queue[TaskStartMessage]] = {}
    
    def create_process_pool(self):
        self.end_queue = Queue()
        
        for _ in range(self.pool_size):
            process_queue = Queue()
            process = Process(target=Flow.wait_for_task, args=(process_queue, self.end_queue))
            self.process_queues[process.name] = process_queue
            self.processes.append(process)

        self.busy = [False] * len(self.processes)

    def free_process(self, process_name: str):
        idx = next( idx for idx, process in enumerate(self.processes) if process.name == process_name)
        self.busy[idx] = False
        
    def __iter__(self):
        return ProcessPoolIterator(self.processes, self.busy)
    
    def __enter__(self):
        self.create_process_pool()
        
        for process in self.processes:
            process.start()
            
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        print(exc_value)
        
        for queue in self.process_queues.values():
            queue.put(ProcessEndMessage())
        
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
        
        self.busy[self.item] = True
        return self.processes[self.item]
    

       
class Flow:
    def __init__(self, task_fns, flow_name: str, cpu_cores = None, flow_input = dict()):
        # TODO: it should not be possible to create a cycle ...
        #       if the same task is called multiple times give it a sequence number e.g. my_task-0 ... my_task-n
        self.task_fns = dict(task_fns)
        self.flow_name = flow_name
        self.flow_input = flow_input
        self.graph: dict[Task, set[Task]] = TaskDict({Task(name, fn): set() for name, fn in task_fns})
        self.waiting_tasks: deque[Task] = deque([])
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
                edge_name = set(head.output_variables) & set(tail.inputs)
                graph.edge(head.task_name, tail.task_name, label=str(edge_name))
            
            if not self.graph[head]:
                graph.node(head.task_name)
        
        graph.render(self.flow_name, cleanup=True)

    def set_flow_input(self) -> Task:
        flow_task: Task = self.graph[self.flow_name]
        for task in self.graph[flow_task]:
            for variable, _ in task.inputs.items():
                if variable in self.flow_input:
                    task.inputs[variable] = self.flow_input[variable]

        return flow_task
        
    def get_entry_tasks(self) -> set[Task]:
        return set(task for task in self.graph if not task.inputs and task.task_name != self.flow_name)
    

    def pass_outputs_to_next_task(self, task: Task):
        for next_task in self.graph[task]:
            next_task.inputs = {**next_task.inputs, **task.outputs}
                
    def is_task_ready(self, task: Task) -> bool:
        # TODO: Technically user could return None or 0 in a variable and this would not work ...
        #       I think the plan is to return an object that contains STATE and data.
        # This still returns True for no inputs
        return all(input for input in task.inputs.values())
    
    def submit_next_task(self, tasks: set[Task], pool: ProcessPool):
        processes = iter(pool)    
        for next_task in tasks:
            if not self.is_task_ready(next_task):
                continue
                
            process = next(processes, None)
            # There is no ready process available
            if not process:
                self.waiting_tasks.append(next_task)
            # Queue the next task to start
            else:
                queue = pool.process_queues[process.name]
                queue.put(TaskStartMessage(next_task))

    
    def submit_waiting_tasks(self, pool: ProcessPool):
        for _ in range(len(self.waiting_tasks)):
            # TODO: This needs to be tested
            task = set(self.waiting_tasks.popleft())
            self.submit_next_task(task, pool)
            

    def flow_run_complete(self) -> bool:
        return all(task.state for task in self.graph if task.task_name != self.flow_name)
            
    # TODO: This might need to be a seperate class e.g. TaskRunner
    # NOTE: One major difference is that with Processes vs threads we will need to reload the modules ...
    #       In that process.    
    @staticmethod
    def wait_for_task(process_queue: Queue[TaskStartMessage], end_queue: Queue[TaskEndMessage]):
        process_name = mp.current_process().name
        
        while message := process_queue.get():

            if isinstance(message, ProcessEndMessage):
                break
        
            task = message.task
            inputs, output_variables, fn = task.inputs, task.output_variables, task.fn
            # TODO: Note inputs assumes only *args and not **kwargs currently
            output = fn(**inputs)

            # TODO: Create a setter for task.outputs instead of having this here
            if isinstance(output, tuple):
                task.outputs = dict(zip(output_variables, output))
            elif output:
                [output_variables] = output_variables
                task.outputs = {output_variables: output}
            
            # TODO: The task passed in and out of the queue does not have the same mememory address ...
            # so we cannot use this task in any way on the main thread.
            # To avoid confusion it might be a better idea to only pass the data that needs to be updated ...
            # in that task instead of the task object itself.
            end_queue.put(TaskEndMessage(process_name, task))

    
    def run(self):

        with ProcessPool(self.pool_size) as pool:
            # Set task input for tasks that are tail nodes to the flow input
            flow_task = self.set_flow_input()
            
            # Get entrypoint tasks that have in-deg = 0
            entry_tasks = self.get_entry_tasks()
            
            # Start the entry point tasks
            self.submit_next_task(entry_tasks, pool)
            self.submit_next_task(self.graph[flow_task], pool)
       
            while message := pool.end_queue.get():
                # Set the process as free/not busy
                pool.free_process(message.process_name)
                # There is at least one free process available, so submit any waiting tasks
                self.submit_waiting_tasks(pool)

                task = message.task
                self.graph[task.task_name].state = True # Temporary
                self.pass_outputs_to_next_task(task)
                self.submit_next_task(self.graph[task], pool)
                
                if self.flow_run_complete():
                    break
                
    
if __name__ == "__main__":
      import engine      


