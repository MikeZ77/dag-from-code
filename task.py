from __future__ import annotations

from enum import Enum, auto
from typing import Callable, Any
from dataclasses import dataclass

# TODO: There needs to be Task and Flow STATES
# The state objects get returned through the message
# Eg ...

class TaskState(Enum):
    PENDING = auto()
    RUNNING = auto()
    SUCCESS = auto()
    FAILED = auto()
    RETRYING = auto()


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

@dataclass
class TaskData:
    state: TaskState
    result: Any
    message: str

class Task:
    # TODO: # A task should take a wait_for arg s.t. it waits for those listed tasks to finish
    def __init__(self, name: str, fn: Callable):
        self.task_name = name
        self.fn = fn 
        # For now, we just have state complete and not complete ...
        # until and actual state system is implemented
        self.state = False
        # TODO: we need to differentiate between *args and **kwargs
        # {"variable_name_1": payload, "variable_name_2": payload ...} 
        self.inputs = {}
        self.outputs = {}
        # We need to return a payload with the name of the variables. So the output of fn can be mapped to self.outputs.
        self.output_variables = [] # [variable_name_1, variable_name_2]
        
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.task_name == other.task_name
        return False

    def __hash__(self):
        return hash(self.task_name)
    
    def map():
        ...
        
    def async_map():
        # TODO: The plan for this is to run coroutines on the event loop ...
        # expects an Iterable
        ...
    
    def run(self):
        ...