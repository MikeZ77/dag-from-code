# TODO: Allow the engine to use either processes or threads based on user preference

from multiprocessing import Process, Queue
from collections.abc import Iterable, Iterator

from engine.task import TaskEndMessage, TaskStartMessage, ProcessEndMessage
from engine.task_runner import TaskRunner


class ProcessPool(Iterable):
    def __init__(self, /, runner: TaskRunner, pool_size: int):
        self.pool_size = pool_size
        # NOTE: right now the runner is only a static method
        self.runner = runner
        self.processes: list[Process] = []
        self.busy: list[bool] = []
        self.end_queue: Queue[TaskEndMessage] = None
        self.process_queues: dict[str, Queue[TaskStartMessage]] = {}
    
    def create_process_pool(self):
        self.end_queue = Queue()
        
        for _ in range(self.pool_size):
            process_queue = Queue()
            process = Process(target=TaskRunner.wait_for_task, args=(process_queue, self.end_queue))
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
        while self.item < len(self.busy) and self.busy[self.item]:
            self.item += 1
        
        if self.item >= len(self.processes):
            raise StopIteration
        
        self.busy[self.item] = True
        return self.processes[self.item]