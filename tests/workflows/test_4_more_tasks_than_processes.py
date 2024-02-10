from engine.engine import flow, task

@task
def parent_task():
    return 1
    
@task
def child_task_1(input):
    return input + 1
    
    
@task
def child_task_2(input):
    return input + 2   


@task
def child_task_3(input):
    return input + 3


@task
def child_task_4(input):
    return input + 4


@task
def child_task_5(input):
    return input + 5


@task
def child_task_6(input):
    return input + 6


@task
def aggregate_task(a, b, c, d, e, f):
    print(sum([a, b, c, d, e, f]))


@flow(cpu_cores=2)
def workflow():
    output = parent_task()
    a = child_task_1(output)
    b = child_task_2(output)
    c = child_task_3(output)
    d = child_task_4(output)
    e = child_task_5(output)
    f = child_task_6(output)
    aggregate_task(a, b, c, d, e, f)
    
    
def test_more_tasks_than_processes():
    workflow()

if __name__ == "__main__":
    workflow()