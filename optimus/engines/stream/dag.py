class Vertex:
    def __init__(self, key, operation=None):
        self.key = key
        self.operation = operation
        self.connections = {}

    def add_neighbor(self, neighbor, weight=0):
        self.connections[neighbor] = weight

    def __str__(self):
        return f'{self.key} connected to: {[x.key for x in self.connections]}'


class Graph:
    def __init__(self, dsk=None):
        self.vert_list = {}
        self.vert_count = 0
        if dsk:
            self.build_from_dict(dsk)

    def add_vertex(self, key, operation=None):
        self.vert_count += 1
        new_vertex = Vertex(key, operation)
        self.vert_list[key] = new_vertex
        return new_vertex

    def get_vertex(self, key):
        if key in self.vert_list:
            return self.vert_list[key]
        else:
            return None

    def add_edge(self, from_key, to_key, weight=0):
        if from_key not in self.vert_list:
            self.add_vertex(from_key)
        if to_key not in self.vert_list:
            self.add_vertex(to_key)
        self.vert_list[from_key].add_neighbor(self.vert_list[to_key], weight)

    def build_from_dict(self, dsk):
        for key, value in dsk.items():
            operation, args = value[0], value[1:]
            vertex = self.add_vertex(key, operation)
            if not isinstance(args, (list, tuple)):
                args = (args,)
            for arg in args:
                if isinstance(arg, str) and arg in dsk:
                    self.add_edge(arg, key)

    def __iter__(self):
        return iter(self.vert_list.values())


def execute_dag(graph, input_data):
    visited = set()
    results = {}

    def visit(vertex):
        nonlocal input_data
        if vertex not in visited:
            visited.add(vertex)
            for neighbor in vertex.connections:
                visit(neighbor)
            if vertex.operation:
                if isinstance(input_data, dict):
                    args = tuple(
                        input_data[arg_key] if arg_key in input_data else arg_key for arg_key in vertex.operation[1])
                    results[vertex.key] = vertex.operation[0](*args)
                else:
                    results[vertex.key] = vertex.operation(input_data)

    for vertex in graph:
        visit(vertex)

    return results


# Define some simple operations
def load(filename):
    return f"Loaded {filename}"


def clean(data):
    return f"Cleaned {data}"


def analyze(data_list):
    return f"Analyzed {', '.join(data_list)}"


def store(result):
    return f"Stored {result}"


# Create the DAG using a dictionary
dsk = {
    'load-1': (load, 'myfile.a.data'),
    'load-2': (load, 'myfile.b.data'),
    'load-3': (load, 'myfile.c.data'),
    'clean-1': (clean, 'load-1'),
    'clean-2': (clean, 'load-2'),
    'clean-3': (clean, 'load-3'),
    'analyze': (analyze, ['clean-%d' % i for i in [1, 2, 3]]),
    'store': (store, 'analyze')
}

# Build and execute the DAG
dag = Graph(dsk)
results = execute_dag(dag, {})

# Print the results
for key, result in results.items():
    print(f'{key}: {result}')
