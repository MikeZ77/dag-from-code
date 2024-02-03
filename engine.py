# TODO: # A task should take a wait_for arg s.t. it waits for those listed tasks to finish
# TODO: This file should "orchistrate" the entire flow run in steps. See the pipeline dp: ...
# https://learn.microsoft.com/en-us/previous-versions/msp-n-p/ff963548(v=pandp.10)?redirectedfrom=MSDN

import ast
import inspect
import workflow

from typing import Any
from collections import defaultdict
from graph import Graph, Edge
    
# TODO: Instead of this, the functions should be "registered"  
# Likewise, engine.py would be called from the flow function registered @flow instead of "import workflow"   
tasks = inspect.getmembers(workflow, predicate=inspect.isfunction)
source_code = inspect.getsource(workflow.workflow)
tree = ast.parse(source_code)
# TODO: The graph would be instantiated from the Flow (is part of the flow), so the flow would be responsible ...
# For passing the flow input to it. For now just hardcode an input to pass in.
graph = Graph(task_fns=tasks, flow_name="workflow", cpu_cores=4, flow_input={"input": 2})
print(ast.dump(tree, indent=4))

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


visitor = BuildDAG()
visitor.visit(tree)
visitor.construct_edges()

for edge, variables in visitor.edges.items():
    graph.add_edge(variables, edge)

graph.draw()
graph.run()



