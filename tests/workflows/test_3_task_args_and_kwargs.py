from engine.engine import flow, task

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
def positional_only_args_and_kwargs(one, two_arg, three_kw, four_kw):
    print("arbitrary_args_and_kwargs")
    assert one == 1
    assert two_arg == 2
    assert three_kw == 3
    assert four_kw == 4
    print(one, two_arg, three_kw, four_kw)


@task
def args_and_kwargs(one, /, two_arg, *, three_kw, four_kw):
    print("args_and_kwargs")
    print(one, two_arg, three_kw, four_kw)

@task
def arbitrary_kwargs(**kwargs):
    print("arbitrary_kwargs")
    print(kwargs)


@task
def arbitrary_args_and_kwargs(first, *args, three_kw, **kwargs):
    print("args_and_kwargs_task")
    print(first, args, three_kw, kwargs)


@flow(cpu_cores=6)
def workflow():
    one, two, three, four = create_outputs()
    output_name_is_input_name(one)
    optional_kwarg(first=one)
    output_is_not_input_name(one)
    args_and_kwargs(one, two, three_kw=three, four_kw=four)
    positional_only_args_and_kwargs(one, two, three_kw=three, four_kw=four)
    arbitrary_args_and_kwargs(one, two, three_kw=three, four_kw=four)
    
    # Alternatively can be called like this:    
    # arbitrary_args_and_kwargs(first=one, three_kw=three, four_kw=four)

    
def test_task_args_and_kwargs():
    workflow()    