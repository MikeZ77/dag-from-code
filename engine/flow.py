from __future__ import annotations

from collections import deque

from engine.task import Task, TaskStartMessage
from engine.graph import Graph
from engine.pool import ProcessPool
    
# TODO: Just like a Task has a state_handler, the flow can have a terminal_state_handler.
class Flow:
    def __init__(self, flow_name: str, graph: Graph, process_pool: ProcessPool, flow_input):
        self.flow_name = flow_name
        self.flow_input = flow_input
        self.graph = graph._graph
        self.process_pool = process_pool
        self.waiting_tasks: deque[Task] = deque([])

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
            # print("DEBUG: ", task.task_name, "- >", next_task.task_name)
            next_task.inputs.update((var, value) for var, value in task.outputs.items() if var in next_task.inputs)
                
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
            task = set([self.waiting_tasks.popleft()])
            self.submit_next_task(task, pool)
            

    def flow_run_complete(self) -> bool:
        return all(task.state for task in self.graph if task.task_name != self.flow_name)
            
    def run(self):

        with self.process_pool as pool:
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
                



