# TODO: Automate the number of processes created by checking for bottlenecks (max number of siblings) using BFS
# TODO: it should not be possible to create a cycle ...
#       if the same task is called multiple times give it a sequence number e.g. my_task-0 ... my_task-n

import ast
import inspect

from graphviz import Digraph
from typing import Any, Type
from collections import defaultdict, abc
from dataclasses import dataclass
from context import execution_context as ctx
from task import Task
    
# tasks = inspect.getmembers(workflow, predicate=inspect.isfunction)
# source_code = inspect.getsource(workflow.workflow)
# tree = ast.parse(source_code)
# graph = Flow(task_fns=tasks, flow_name="workflow", cpu_cores=4, flow_input={"input": 2})
# print(ast.dump(tree, indent=4))

class BuildDAG(ast.NodeVisitor):
    def __init__(self):
        self.head_nodes = defaultdict(list)       # "task_2": ["a"]
        self.tail_nodes = defaultdict(list)       # "a": ["task_3"]
        self.edges = defaultdict(list)            # Edge(head='task_1', tail='task_2'): ['a']
        self.isolated_nodes = set()               # "task_4"
    
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
        # TODO: Validate that this function is a task
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
       
        if not isinstance(node.value, ast.Call):
            return
        
        func: ast.Name = node.value.func
        fn_args: list[ast.Name] = node.value.args
        fn_name = func.id
        
        # The node is a tail node
        if fn_args:
            for param in fn_args:
                self.tail_nodes[param.id].append(fn_name)
            
        # TODO: I dont think we need this since the graph has access to the tasks ...
        # and can determine if a node is isolated.
        else:
            self.isolated_nodes.add(fn_name)
            
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

        
    def add_edge(self, variables: list[str], edge: Edge):
        head, tail = edge.head, edge.tail
        head_task: Task = self._graph[head]
        tail_task: Task = self._graph[tail]
        
        self._graph[head_task].add(tail_task)
        tail_task.inputs = {var: None for var in variables}
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
            graph.add_edge(variables, edge)