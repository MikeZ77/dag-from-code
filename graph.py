# TODO: Automate the number of processes created by checking for bottlenecks (max number of siblings) using BFS
# TODO: Allow the engine to use either processes or threads based on user preference
# TODO: The "Graph" is really the "Flow". However, this may be too much functionality. It may make sense to ...
# create the graph seperately and inject it into the Flow from engine.py 

import networkx as nx
import matplotlib.pyplot as plt
import multiprocessing as mp

from multiprocessing import Process, Queue
from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class Edge:
    head: str
    tail: str   

class Task:
    def __init__(self, name, fn):
        self.task_name = name
        self.fn = fn 
        self.inputs = {} # {"variable_name": payload, ...} 
        self.outputs = {}
        
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
    def __init__(self, task_fns, draw_mode = False, cpu_cores = None):
        # TODO: it should not be possible to create a cycle ...
        # if the same task is called multiple times give it a sequence number e.g. my_task-0 ... my_task-n
        self.task_fns = {name: fn for name, fn in task_fns }
        self.graph = {task_name: set() for task_name, _ in task_fns}
        self.draw_mode = draw_mode
        self.graph_ = nx.DiGraph() if self.draw_mode else None # We only keep this for the purpose of drawing the graph
        self.pool_size = mp.cpu_count() if not cpu_cores else cpu_cores
        self.processes_pool: list[Process] = []
        self.message_queue = Queue()
        
    def add_edge(self, edge: Edge):
        head, tail = edge.head, edge.tail
        task = Task(tail, self.task_fns[tail])
        self.graph[head].add(task)
        
        if self.draw_mode:
            self.graph_.add_edge(head, tail)
            
    def draw(self):
        if self.draw_mode:
            position = nx.spring_layout(self.graph_)
            nx.draw(self.graph_, position)
            # Note: On WSL so not worth doing the intsalls to get this to plt.show()
            # Note: Really, the graph is the flow, so the png can be named accordingly
            plt.savefig("my_flow.png")

    @staticmethod
    def wait_for_task(message_queue: Queue, num):
        message_queue.put("Created process: " + str(num))
    
    def run(self):
        # TODO: A Task is run once all its inputs have been recieved
        # 1. message_queue.get() returns message: {pid: 1234, task_name: "task_one", output: {out_one: 123, out_two: "abc"}} (dataclass) 
        # 2. Set the busy flag in the process_pool for that process to False
        # 2. then iterate through "task_one" 's children:
        #   a. for the task, check if it has the corresponding input and set it.
        #   b. if all inputs are set, then the task is ready to be run 
        #   c. Set the busy flag in the process_pool for that process to True
        
        # ...
        
        for i in range(self.pool_size):
            process = Process(target=self.wait_for_task, args=(self.message_queue, i))
            process.start()
            self.processes_pool.append(process)
        
        # TODO: We will have a while loop here waiting for all Tasks to be finished
        count = 1
        while output := self.message_queue.get():
            print(output)
            count += 1
            if count > self.pool_size:
                break
                
        # Cleanup
        self.message_queue.close()
        self.message_queue.join_thread()
        for process in self.processes_pool:
            process.join()