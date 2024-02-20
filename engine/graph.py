# TODO: Automate the number of processes created by checking for bottlenecks (max number of siblings) using BFS
# TODO: it should not be possible to create a cycle ...
#       if the same task is called multiple times give it a sequence number e.g. my_task-0 ... my_task-n

import ast
import inspect

from graphviz import Digraph
from typing import Type
from collections import defaultdict, abc
from dataclasses import dataclass

from engine.context import execution_context as ctx
from engine.task import Task
from engine.exceptions import UnregisteredTaskCalled, MultipleCallsInsideIterable
    

class BuildDAG(ast.NodeVisitor):
    def __init__(self):
        
        self.head_nodes = defaultdict(list)                             # "task_2": ["a"]
        self.tail_nodes = defaultdict(list)                             # "a": ["task_3"]
        self.edges: dict[Edge, list[str]] = defaultdict(list)           # Edge(head='task_1', tail='task_2'): ['a']
        self.kwargs = defaultdict(dict)                                 # {"task_name": {kwarg_value: kwarg_name}} 
        self.output_variables = defaultdict(list)                       # {"task_name": [output_variable_name]} 
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
        assignor = node.value
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
            self.output_variables[fn_name].extend(variables)

        # The head node has out-deg == 1
        if isinstance(assignee, ast.Name):
            self.head_nodes[fn_name].append(assignee.id)
            self.output_variables[fn_name].append(assignee.id)
        
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
    def __init__(self, task_names: list[str], flow_name: str, module_tree: ast.Module):
        self.task_names = task_names
        self.flow_name = flow_name
        self.module_tree = module_tree
        self.flow_lineno = self._get_flow_func_lineno()

    def _get_flow_func_lineno(self):
        for node in ast.walk(self.module_tree):
            if isinstance(node, ast.FunctionDef) and node.name == self.flow_name:
                # Includes a 2 line offset since the flow lineno overlaps
                return node.lineno - 2
    
    def visit_Assign(self, node):
        assignor = node.value
        lineno = self.flow_lineno + node.lineno
        
        if isinstance(assignor, ast.Tuple) or isinstance(assignor, ast.List):
            if any(isinstance(elt, ast.Call) for elt in assignor.elts):
                raise MultipleCallsInsideIterable(message="Multiple calls inside iterable.", 
                                    lineno=lineno, 
                                    additional_info="A single task can only be called per line."
                                )
        
        if isinstance(assignor, ast.Call) and assignor.func.id not in self.task_names:
            lineno = lineno
            raise UnregisteredTaskCalled(message=f"Unregsitered callable {assignor.func.id} called inside flow scope.", 
                                   lineno=lineno, 
                                   additional_info="Functions or classes called inside the flow scope must be registered @task or task(fn)"
                                )
                

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
        tail_task.inputs.update((var, None) for var in variables)
        

    def __repr__(self):
        output = []
        for node in self._graph:
            line = []
            line.append(f"| {node.task_name} |")
            for child in self._graph[node]:
                line.append(f"({',' .join(child.inputs)}) [{child.task_name}]")

            output.append(line)
                
        for idx, line in enumerate(output):
            output[idx] = " -> ".join(line) + " -> None"
            
        return "\n".join(line for line in output)    
        
    
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
        flow_module = inspect.getmodule(ctx.flow_fn)
        module_source_code = inspect.getsource(flow_module)
        flow_source_code = inspect.getsource(ctx.flow_fn)
        self.flow_name = ctx.flow_fn.__name__
        self._tree = ast.parse(flow_source_code)
        self._module_tree = ast.parse(module_source_code)
        self.tasks = list(ctx.tasks.values())
        self._dag_builder = dag_builder
        self._dag_validator = dag_validator
        # print(ast.dump(self._tree, indent=4))
        # print()
        
    @classmethod
    def from_code(cls):
        graph_build = cls(BuildDAG, ValidateDAG)
        graph = graph_build._build_graph()
        graph_build._validate_flow()
        dag = graph_build._build_dag()
        dag.construct_edges()
        graph_build._update_task_fn_information(dag)
        graph_build._add_edges(graph, dag)
        return graph
        
    
    def _build_graph(self) -> Graph:
        return Graph(self.tasks)
        
    def _validate_flow(self):
        task_names = [task.task_name for task in self.tasks]
        valid_dag = self._dag_validator(task_names, self.flow_name, self._module_tree)
        valid_dag.visit(self._tree)
        
        
    def _build_dag(self) -> BuildDAG:
        dag = self._dag_builder()
        dag.visit(self._tree)
        return dag
        
    def _update_task_fn_information(self, dag: BuildDAG):
        for task in self.tasks:
            if task.task_name in dag.kwargs:
                task.fn_kwargs = dag.kwargs[task.task_name]
            
            if task.task_name in dag.output_variables:
                task.output_variables = dag.output_variables[task.task_name]


    def _add_edges(self, graph: Graph, dag: BuildDAG):
        for edge, variables in dag.edges.items():
            graph.add_edge(variables, edge)