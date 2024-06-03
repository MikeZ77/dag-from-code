## dag-from-code

This is a workflow engine prototype like Prefect or Airflow. Tasks are registered and added to a single flow function. Tasks cannot call other tasks and only tasks can run inside a flow. The flow function does not actually execute. Instead, a DAG (Directed Acyclic Graph) is generated where a vertex is defined by a task and an edge is defined by a task output variable being passed to another task. The flow runs in parallel by haivng each task function execute in a seperate process.

Below is a simple workflow example. More examples can be found in the `tests/workflows` folder.

```python
@task
def get_two():
    return 2

@task
def power(num):
    return num**2

@task
def log(value):
    print(value)

@flow
def my_simple_workflow():
    two = get_two()
    four = power(two)
    log(four)

if __name__ == "__main__":
    my_simple_workflow()

```
