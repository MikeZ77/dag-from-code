import sys

class FlowRunException(Exception):
    """
    Base exception for all flow run execution errors.
    """

class FlowValidationError(Exception):
    """
    Base exception for all DAG validation errors.
    """
    def __init__(self, *, message: str, additional_info: str = None, lineno: int):
        super().__init__(message)
        # Just output the error and not the entire traceback
        sys.tracebacklimit = 0
        self.lineno = lineno
        self.additional_info = additional_info
        
    def __str__(self):
        return  f"""
                message={super().__str__()} 
                additional_info={self.additional_info}
                lineno={self.lineno}
                """

class UnregisteredTaskCalled(FlowValidationError):
    """
    Raised when a function or class is called inside the flow scope and has not been registered as a task.
    """
    
class MultipleCallsInsideIterable(FlowValidationError):
    """
    Raised when callables are called inside an iterable in the flow scope.
    """

class FlowNotFound(FlowValidationError):
    ...