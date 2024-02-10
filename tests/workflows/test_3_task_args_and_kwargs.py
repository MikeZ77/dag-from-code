from engine.engine import flow, task

@task
def create_outputs():
    return 1, 2, 3, 4


@task
def output_name_is_input_name(one):
    print(one)


def output_does_is_not_input_name(first):
    print(first)


@task
def args_task(*args):
    print(args)
    
    
@task
def kwargs_task(**kwargs):
    print(kwargs)


@task()
def args_and_kwargs_task(first, *args, third, **kwargs):
    print(first, args, third, kwargs)


@flow()
def workflow():
    one, two, three, four = create_outputs()
    output_name_is_input_name(one)
    output_does_is_not_input_name(one)
    args_task(one, two, three)
    kwargs_task(one, two, three)
    args_and_kwargs_task(one, two, three=three, four=four)
    
    
def test_task_args_and_kwargs():
    workflow()    