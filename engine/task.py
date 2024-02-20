from __future__ import annotations

from inspect import signature, Parameter
from enum import Enum, auto
from typing import Callable, Any
from dataclasses import dataclass, field


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
class TaskInput:
    args: list = field(default_factory=list)
    kwargs: dict = field(default_factory=dict) 

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

# TODO: A task can be provided a "state_handler" by the client which gets run after the task function has run ...
#       this callback Takes the current state and can return a new state.
#       multiple state handlers can also be chained together.
class Task:
    # TODO: # A task should take a wait_for arg s.t. it waits for those listed tasks to finish
    def __init__(self, name: str, fn: Callable):
        self.task_name = name
        self.fn = fn 
        self.fn_kwargs = {}
        # For now, we just have state complete and not complete ...
        # until and actual state system is implemented
        self.state = False
        # All task inputs get translated to kwargs
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
    
    def __repr__(self):
        return f"""     Task(
            {self.task_name=}
            {self.inputs=}
            {self.fn_kwargs=}
            {self.output_variables=}
        )
        """
    
    def _translate_args(self, inputs: TaskInput):
        parameters = signature(self.fn).parameters.values()
        var_positional_idx = next((idx for idx, param in enumerate(parameters) if param.kind == Parameter.VAR_POSITIONAL), -1) 
        params = ((idx, param) for idx, param in enumerate(parameters) if param.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD, Parameter.VAR_POSITIONAL))
        
        for pos_arg in (input for input in self.inputs if input not in self.fn_kwargs):
            idx, param = next(params)

            if param.kind in (Parameter.POSITIONAL_ONLY, Parameter.VAR_POSITIONAL):
                inputs.args.append(self.inputs[pos_arg])
                
            elif param.kind == Parameter.POSITIONAL_OR_KEYWORD and idx < var_positional_idx:
                inputs.args.append(self.inputs[pos_arg])
                
            elif param.kind == Parameter.POSITIONAL_OR_KEYWORD:
                inputs.kwargs[param.name] = self.inputs[pos_arg]

        return inputs
        
    
    def _translate_kwargs(self, inputs: TaskInput):
        for key, value in self.inputs.items():
            if key in self.fn_kwargs:
                inputs.kwargs[self.fn_kwargs[key]] = value

        return inputs
    
    def _update_outputs(self, output):
        if isinstance(output, tuple):
            self.outputs = dict(zip(self.output_variables, output))
        elif output:
            [output_variables] = self.output_variables
            self.outputs = {output_variables: output}

    
    def map():
        ...
        
    def async_map():
        # TODO: The plan for this is to run coroutines on the event loop ...
        # expects an Iterable
        ...
    
    def run(self):
        inputs = TaskInput()
        inputs = self._translate_args(inputs)
        inputs = self._translate_kwargs(inputs)
        output = self.fn(*inputs.args, **inputs.kwargs)
        self._update_outputs(output)
        
