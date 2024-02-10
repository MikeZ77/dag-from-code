# TODO: Automate the number of processes created by checking for bottlenecks (max number of siblings) using BFS
# TODO: it should not be possible to create a cycle ...
#       if the same task is called multiple times give it a sequence number e.g. my_task-0 ... my_task-n

import ast
import inspect

from graphviz import Digraph
from typing import Any, Type
from collections import defaultdict, abc
from dataclasses import dataclass

from engine.context import execution_context as ctx
from engine.task import Task
    

class BuildDAG(ast.NodeVisitor):
    def __init__(self):
        
        self.head_nodes = defaultdict(list)                             # "task_2": ["a"]
        self.tail_nodes = defaultdict(list)                             # "a": ["task_3"]
        self.edges: dict[Edge, list[str]] = defaultdict(list)           # Edge(head='task_1', tail='task_2'): ['a']
        self.kwargs = defaultdict(dict)                                 # {"task_name": {kwarg_value: kwarg_name}}   
        # TODO: Keep track of variables and handle duplcaite variables i.e. variables that a re-assigned in the flow
        
    def visit_FunctionDef(self, node):
        # TODO: Validate that this is a flow function
        args: ast.arguments = node.args
        fn_args: list[ast.arg] = args.args
        fn_name = node.name
        
        self.head_nodes[fn_name] = [arg.arg for arg in fn_args]
        self.generic_visit(node)
        
    def visit_Assign(self, node):
        # An Assign creates a head Node and can also be a tail Node
        assignor: Any  = node.value
        assignee: ast.Name | ast.Tuple = node.targets[0]

        # This cannot be a Task since the assingnor is not a function call
        if not isinstance(assignor, ast.Call):
            return
        
        fn: ast.Name = assignor.func
        fn_name = fn.id
        fn_args: list[ast.Name] = assignor.args
        
        # The head node has out-deg > 1
        if isinstance(assignee, ast.Tuple):
            names: list[ast.Name] = assignee.elts
            variables = [name.id for name in names]
            self.head_nodes[fn_name].extend(variables)

        # The head node has out-deg == 1
        if isinstance(assignee, ast.Name):
            self.head_nodes[fn_name].append(assignee.id)
        
        # Check if this head node is also a tail node (has params)
        if fn_args:
            for param in fn_args:
                self.tail_nodes[param.id].append(fn_name)
            
        self.generic_visit(node)
        
    def visit_Expr(self, node):
        
        # The expression cannot be a task
        if not isinstance(node.value, ast.Call):
            return
        
        func: ast.Name = node.value.func
        fn_args: list[ast.Name] = node.value.args
        fn_kwargs: list[ast.keyword] = node.value.keywords
        fn_name = func.id
        
        # we get the tail node arg so it can be mapped back to the value of the head node output
        if fn_args:
            for arg in fn_args:
                self.tail_nodes[arg.id].append(fn_name)
        
        # tail node kwarg 
        if fn_kwargs:
            # get the kwarg value so it can be mapped back to the value of the head node output
            for kwarg in fn_kwargs:
                self.tail_nodes[kwarg.value.id].append(fn_name)

            # keep track of the kwarg name for task input
            self.kwargs[func.id][kwarg.value.id] = kwarg.arg
            
        self.generic_visit(node)
    
    def construct_edges(self):
        for head_node, edge_names in self.head_nodes.items():
            for edge_name in edge_names:
                tail_nodes = self.tail_nodes[edge_name]
                for tail_node in tail_nodes:
                    edge = Edge(head=head_node, tail=tail_node)
                    self.edges[edge].append(edge_name)



class ValidateDAG(ast.NodeVisitor):
    ...


@dataclass(eq=True, frozen=True)
class Edge:
    head: str
    tail: str  


class TaskGraph(abc.Mapping):
    def __init__(self, graph: dict[Task, set[Task]]):
        self.graph = graph
        self.map = { task.task_name: task for task in graph}
        
    def __getitem__(self, key: str | Task) -> set[Task]:
        if isinstance(key, str):
            return self.map[key]
        else:
            return self.graph[key]
        
    def __iter__(self):
        return iter(self.graph)
        
    def __len__(self):
        return len(self.graph)
        

class Graph:
    def __init__(self, tasks: list[Task]):
        self._graph = TaskGraph({task: set() for task in tasks})

        
    def add_edge(self, variables: list[str], edge: Edge, fn_kwargs: dict):
        head, tail = edge.head, edge.tail
        head_task: Task = self._graph[head]
        tail_task: Task = self._graph[tail]
        
        tail_task.fn_kwargs = fn_kwargs
        
        self._graph[head_task].add(tail_task)
        tail_task.inputs = {var: None for var in variables}
        if not set(variables).issubset(set(head_task.output_variables)):
            head_task.output_variables.extend(variables)

    def nodes(self) -> set[Task]:
        return set(self._graph.keys())

    def children(self, task: Task) -> set[Task]:
        return self._graph[task]

    def draw(self, flow_name: str):
        graph = Digraph(format='pdf')
        for head in self._graph:
            for tail in self._graph[head]:
                edge_name = set(head.output_variables) & set(tail.inputs)
                graph.edge(head.task_name, tail.task_name, label=str(edge_name))
            
            if not self._graph[head]:
                graph.node(head.task_name)
        
        graph.render(flow_name, cleanup=True)
   
    
class BuildGraph:
    """ Orchistrates building the DAG (directed acyclic graph) i.e. the graph"""
    def __init__(self, dag_builder: Type[BuildDAG], dag_validator: Type[ValidateDAG]):
        self._flow_source_code = inspect.getsource(ctx.flow_fn)
        self._tree = ast.parse(self._flow_source_code)
        self._dag_builder = dag_builder
        self._dag_validator = dag_validator
        print(ast.dump(self._tree, indent=4))
        print()
        
    @classmethod
    def from_code(cls):
        tasks = list(ctx.tasks.values())
        graph = Graph(tasks)
        graph_build = cls(BuildDAG, ValidateDAG)
        graph_build._validate_flow()
        dag = graph_build._build_dag()
        dag.construct_edges()
        graph_build._add_edges(graph, dag)
        return graph
        
        
    def _validate_flow(self):
        ...
        
        
    def _build_dag(self) -> BuildDAG:
        dag = self._dag_builder()
        dag.visit(self._tree)
        return dag
        
        
    def _add_edges(self, graph: Graph, dag: BuildDAG):
        for edge, variables in dag.edges.items():
            fn_kwargs = dag.kwargs[edge.tail]
            graph.add_edge(variables, edge, fn_kwargs)