from __future__ import annotations

import multiprocessing as mp

from multiprocessing import Queue

from engine.task import TaskEndMessage, TaskStartMessage, ProcessEndMessage

# TODO: Before a task runs, it should evaluate a "trigger" which evaluates the upstream state and decides if ...
#       the task should run (i.e. True or False). E.g. all_successful, any_failed, all_finished, etc.
# TODO: The TaskRunner should be responsible for performing task retries based on the state returned.
class TaskRunner:
    def __init__(self):
        ...       
         
    @staticmethod
    def wait_for_task(process_queue: Queue[TaskStartMessage], end_queue: Queue[TaskEndMessage]):
        process_name = mp.current_process().name
        
        while message := process_queue.get():

            if isinstance(message, ProcessEndMessage):
                break
        
            task = message.task
            # print(task)
            # TODO: Handle the exception
            task.run()

            # TODO: The task passed in and out of the queue does not have the same memory address ...
            # so we cannot use this task in any way on the main thread.
            # To avoid confusion it might be a better idea to only pass the data that needs to be updated ...
            # in that task instead of the task object itself.
            end_queue.put(TaskEndMessage(process_name, task))