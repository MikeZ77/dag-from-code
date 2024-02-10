from engine.engine import flow, task

# TODO: It would be neat to read task comments and types and display these in a UI graph (if a UI is ever built)
# TODO: Maybe we can "inject" deps into the task i.e. create some kind of simple built in DI framework
# TODO: For more fine grained control, it would be good if a task could be run on the ThreadPool or ProcessPool
# TODO: A task should have a "STATE" handler: e.g. could call another function
# TODO: A task can also be a class. This would allow more complicated behavior and state ...
#       must implement a run() method so the task runner can execute it.
# @task(deps=[])
# @task should register this function as a task

# @task
# def task_1(input):
#     print("Task_1")
#     return input + 1

# @task
# def task_2(a):
#     print("Task_2")
#     return 1, 2

# @task
# def task_3(input):
#     print("Task_3")

# @task
# def task_4(b, c):
#     print(b, c)

# @task
# def task_5():
#     print("I do nothing")

# @flow(draw=False, cpu_cores=4)
# def workflow(input: int = 2):
#     a = task_1(input)
#     b, c = task_2(a)
#     task_3(input)
#     task_4(b, c)
#     task_5()
    
    
@task
def create_outputs():
    return 1, 2, 3, 4


@task
def output_name_is_input_name(one):
    print("output_name_is_input_name")
    print(one)


@task
def optional_kwarg(first):
    print("optional_kwarg")
    print(first)


@task
def output_is_not_input_name(first):
    print("output_is_not_input_name")
    print(first)


@task
def arbitrary_args_and_kwargs(one, two_arg, three_kw, four_kw):
    print("arbitrary_args_and_kwargs")
    print(one, two_arg, three_kw, four_kw)


# @task
# def args_task(*args):
#     print("args_task")
#     print(args)
    
    
# @task
# def kwargs_task(**kwargs):
#     print("kwargs_task")
#     print(kwargs)


# @task
# def args_and_kwargs_task(first, *args, third, **kwargs):
#     print("args_and_kwargs_task")
#     print(first, args, third, kwargs)


@flow()
def workflow():
    one, two, three, four = create_outputs()
    output_name_is_input_name(one)
    optional_kwarg(first=one)
    output_is_not_input_name(one)
    arbitrary_args_and_kwargs(one, two, three_kw=three, four_kw=four)
    # args_task(one, two, three)
    # kwargs_task(one, two, three)
    # args_and_kwargs_task(one, two, three=three, four=four)
    
  
if __name__ == "__main__":
    workflow()

