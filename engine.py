# TODO: # A task should take a wait_for arg s.t. it waits for those listed tasks to finish
# TODO: This file should "orchistrate" the entire flow run in steps. See the pipeline dp: ...
# https://learn.microsoft.com/en-us/previous-versions/msp-n-p/ff963548(v=pandp.10)?redirectedfrom=MSDN

import ast
import inspect
import workflow

from collections import defaultdict
from graph import Graph, Edge
    
# TODO: Instead of this, the functions should be "registered"  
# Likewise, engine.py would be called from the flow function registered @flow instead of "import workflow"   
tasks = inspect.getmembers(workflow, predicate=inspect.isfunction)
source_code = inspect.getsource(workflow.workflow)
root_node = ast.parse(source_code)
graph = Graph(task_fns=tasks, draw_mode=False, cpu_cores=4)
print(ast.dump(root_node, indent=4))

# An Edge is defined as connecting two adjacent Vertices
# E.g. Edge: (Node X, Node Y)

edges = {}
edge_nodes = defaultdict(list)
isolated_nodes = set()
nodes = set()

# walk does an in-order traversel of the ast
for node in ast.walk(root_node):
    print(node)
    # The flow itself can be considered a source node
    if isinstance(node, ast.FunctionDef):
        edges_out = [edge.arg for edge in node.args.args]
        head = node.name

        for edge in edges_out:
            edges[edge] = Edge(head, None)

    if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
        edges_out = node.targets
        head = node.value.func.id
        
        if isinstance(edges_out[0], ast.Tuple):
            edges_out = edges_out[0].elts
        
        for edge in edges_out:
            edges[edge.id] = Edge(head, None)

    
    if (isinstance(node, ast.Expr) or isinstance(node, ast.Assign)) and isinstance(node.value, ast.Call):
        edge_in = node.value.args
        tail = node.value.func.id
        
        for edge in edge_in:
            head = edges[edge.id].head
            edges[edge.id] = Edge(head, tail)

        if not edge_in:
            isolated_nodes.add(tail)
            
        

print(edges)
for edge in edges.values():
    graph.add_edge(edge)

graph.draw()
graph.run()

# # Combines multiple edges from and to the same nodes into a single edge
# for edge_name, edge in edges.items():
#    edge_nodes[edge].append(edge_name) 

# print(edge_nodes)


