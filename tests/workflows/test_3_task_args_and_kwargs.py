from functools import partial

from engine.engine import flow, task

def create_outputs():
    return 1, 2, 3, 4


def output_name_is_input_name(one):
    assert one == 1


def optional_kwarg(first):
    assert first == 1


def output_is_not_input_name(first):
    assert first == 1


def positional_only_args_and_kwargs(one, two_arg, three_kw, four_kw):
    assert one == 1
    assert two_arg == 2
    assert three_kw == 3
    assert four_kw == 4


def args_and_kwargs(one, /, two_arg, *, three_kw, four_kw):
    assert one == 1
    assert two_arg == 2
    assert three_kw == 3
    assert four_kw == 4

def arbitrary_kwargs(**kwargs):
    assert kwargs.get("one_kw") == 1
    assert kwargs.get("two_kw") == 2

def arbitrary_args_and_kwargs(first, *args, three_kw, **kwargs):
    assert first == 1
    assert args[0] == 2
    assert three_kw == 3
    assert kwargs.get("four_kw") == 4


def workflow():
    one, two, three, four = create_outputs()
    output_name_is_input_name(one)
    optional_kwarg(first=one)
    output_is_not_input_name(one)
    args_and_kwargs(one, two, three_kw=three, four_kw=four)
    arbitrary_kwargs(one_kw=one, two_kw=two)
    positional_only_args_and_kwargs(one, two, three_kw=three, four_kw=four)
    arbitrary_args_and_kwargs(one, two, three_kw=three, four_kw=four)
    # Alternatively can be called like this:    
    # arbitrary_args_and_kwargs(first=one, three_kw=three, four_kw=four)

    
def test_task_args_and_kwargs():
    task(create_outputs)
    task(output_name_is_input_name)
    task(optional_kwarg)
    task(output_is_not_input_name)
    task(positional_only_args_and_kwargs)
    task(args_and_kwargs)
    task(arbitrary_kwargs)
    task(arbitrary_args_and_kwargs)
    # cpu_cores=6 so we are not testing more_tasks_than_processes
    flow_ = partial(flow, cpu_cores=6)
    flow_(workflow)()