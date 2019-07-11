from functools import reduce
import networkx as nx


def _ipython_view_pydot(pdot):
    from IPython.display import Image, display
    plt = Image(pdot.create_png())
    # noinspection PyTypeChecker
    display(plt)


def display_graph(graph):
    pdot = nx.drawing.nx_pydot.to_pydot(graph)
    for edge in pdot.get_edges():
        dest = edge.get_destination()
        src = edge.get_source()
        data = graph.get_edge_data(src, dest)
        if data is not None:
            if 'name' in data:
                edge.set_label('{}'.format(data['name']))
    try:
        get_ipython()
        return _ipython_view_pydot(pdot)
    except NameError():
        import matplotlib.image as mpimg
        import matplotlib.pyplot as plt
        from StringIO import StringIO
        sio = StringIO()
        png_str = pdot.create_png(prog='dot')
        sio.write(png_str)
        sio.seek(0)
        img = mpimg.imread(sio)
        # plot the image
        im = plt.imshow(img, aspect='equal')
        return im


def traverse_until(graph, stop_condition, source):
    branch = set()
    for child in graph.successors(source):
        predecessors = graph.predecessors(child)
        if not stop_condition(graph, child) and not any(stop_condition(graph, s) for s in predecessors):
            branch.add(child)
    more = set()
    for child in branch:
        more |= traverse_until(graph, stop_condition, child)
    branch |= more
    if not stop_condition(graph, source):
        branch.add(source)
    return branch


def exclude_aggregates(graph, node):
    return graph.nodes[node]['type'] == 'aggregate'


def get_subgraph_until(graph, stop_condition, source):
    nodes = traverse_until(graph, stop_condition, source)
    return nx.subgraph(graph, nodes).copy()


def split_subgraphs(graph, stop_condition, source):
    good = get_subgraph_until(graph, stop_condition, source)
    bad = nx.subgraph(graph, [n for n in graph.nodes if n not in good.nodes]).copy()
    return good, bad


def find_roots(graph):
    """
    return nodes which you can't traverse down any further
    """
    return [n for n in graph.nodes() if len(list(graph.predecessors(n))) == 0]


def partition_graph(graph, stop_condition, source):
    order = []
    if graph.size() == 0:
        return order

    good, bad = split_subgraphs(graph, stop_condition, source)
    order.append(good)
    isolateds = []

    roots = find_roots(bad)
    for i in roots:
        if stop_condition(graph, i):
            isolated = nx.subgraph(graph, [i]).copy()
            pre = next(graph.predecessors(i))
            attrs = graph.get_edge_data(pre, i)
            isolated.add_node(source, type=graph.nodes()[source]['type'])
            isolated.add_edge(source, i, **attrs)
            isolateds.append(isolated)
    order += isolateds
    bad = nx.subgraph(bad, [n for n in bad.nodes() if n not in roots]).copy()
    for root in find_roots(bad):
        pre = next(graph.predecessors(root))  # assumes it can only be created by one function
        attrs = graph.get_edge_data(pre, root)
        bad.add_node(source, type='source')
        bad.add_edge(source, root, **attrs)
    try:
        order += partition_graph(bad, stop_condition, source)
    except nx.NetworkXException:
        pass
    return order


def ordered_subgraphs(graph, source):
    return partition_graph(graph, exclude_aggregates, source)


class GraphValidationError(Exception):
    pass


class GraphPartitionValidationError(GraphValidationError):
    pass


def validate_graph(graph, source):
    assert isinstance(graph, nx.DiGraph), "Graph must be directed and an instance of Networkx.DiGraph"
    roots = tuple(find_roots(graph))
    assert roots == (source,)
    for node in graph.nodes():
        if node != source:
            attrs = [graph.get_edge_data(pre, node) for pre in graph.predecessors(node)]
            attrs = {(a['action'], a['name']) for a in attrs}
            if len(attrs) != 1:
                raise GraphValidationError("node <{}> cannot be created by more than 1 process: {}".format(node, attrs))
            if graph.nodes()[node]['type'] == 'aggregate':
                for pre in graph.predecessors(node):
                    action = graph.get_edge_data(pre, node)['action']
                    if action != 'agg':
                        msg = "{}-({})->{} : Aggregate nodes can only be made from aggregate actions".format(pre,
                                                                                                             action,
                                                                                                             node)
                        raise GraphValidationError(msg)


def validate_subgraphs(graph, subgraphs, source):
    if frozenset({n for sub in subgraphs for n in sub.nodes()}) != frozenset(graph.nodes()):
        raise GraphPartitionValidationError("Subgraphs do not add up to the input graph: not all nodes will be run")
    err = "Subgraphs are disordered: subgraph-{} has node '{}' which depends on '{}' in subgraph-{}"
    for i, subgraph in enumerate(subgraphs):
        validate_graph(subgraph, source)
        for node in subgraph.nodes():
            if subgraph.nodes()[node]['type'] == 'aggregate':
                if len(subgraph.nodes()) != 2:  # source + agg=2
                    raise GraphPartitionValidationError("An aggregate data product can only be run in isolation")
            if i < len(subgraphs) - 1:
                for pre in graph.predecessors(node):
                    for j, next_subgraph in enumerate(subgraphs[i + 1:], i + 1):
                        if pre in next_subgraph.nodes() and pre != source:
                            raise GraphPartitionValidationError(err.format(i, node, pre, j))


def traverse_in_order(graph, source):
    todo = [i for i in graph.nodes() if i != source]
    done = [source]
    path = []

    while len(todo) > 0:
        old_len = len(todo)
        for node1 in done:
            for node2 in todo:
                accessible = node2 in graph.successors(node1)
                fufilled = all(p in done for p in graph.predecessors(node2))
                if accessible and fufilled:
                    todo = [i for i in todo if i != node2]
                    done.append(node2)
                    path.append((node1, node2))
        assert old_len != len(todo), "No changes occurred"
    return path


def combine_chain(chain, node):
    if node in chain:
        return chain
    return chain + [node]


def action_order(edges):
    return reduce(combine_chain, edges, [])


def edge_order(graph, source):
    edges = traverse_in_order(graph, source)
    return action_order([graph.get_edge_data(*e)['name'] for e in edges])


if __name__ == '__main__':
    G = nx.DiGraph()

    G.add_node('source', type='source')

    G.add_node('M1', type='per-object')
    G.add_edge('source', 'M1', action='map', name='map-1')

    G.add_node('M2', type='per-object')
    G.add_edge('source', 'M2', action='map', name='map-1')

    G.add_node('M3', type='per-object')
    G.add_edge('source', 'M3', action='map', name='map-1')

    G.add_node('F1', type='per-object')
    G.add_edge('M1', 'F1', action='filter', name='filter-1')

    G.add_node('M7', type='per-object')
    G.add_edge('F1', 'M7', action='map', name='map-2')
    G.add_edge('M1', 'M7', action='map', name='map-2')
    G.add_edge('M2', 'M7', action='map', name='map-2')

    G.add_node('E1', type='aggregate')
    G.add_edge('M7', 'E1', action='agg', name='agg-1')

    G.add_node('E2', type='aggregate')
    G.add_edge('M3', 'E2', action='agg', name='agg-2')

    G.add_node('M4', type='per-object')
    G.add_edge('source', 'M4', action='map', name='map-3')

    G.add_node('M5', type='per-object')
    G.add_edge('E2', 'M5', action='map', name='map-4')
    G.add_edge('M4', 'M5', action='map', name='map-4')

    G.add_node('M6', type='per-object')
    G.add_edge('M5', 'M6', action='map', name='map-5')
    G.add_edge('E1', 'M6', action='map', name='map-5')

    G.add_node('M8', type='per-object')
    G.add_edge('M5', 'M8', action='map', name='map-7')

    G.add_node('M9', type='per-object')
    G.add_edge('E4', 'M9', action='map', name='map-8')
    G.add_edge('M8', 'M9', action='map', name='map-8')

    G.add_node('E4', type='aggregate')
    G.add_node('E5', type='aggregate')
    G.add_edge('M7', 'E5', action='agg', name='agg-3')
    G.add_edge('E5', 'E4', action='agg', name='agg-4')
    G.add_edge('E4', 'M6', action='map', name='map-5')

    G.add_node('E3', type='aggregate')
    G.add_edge('M6', 'E3', action='agg', name='agg-5')

    validate_graph(G, 'source')
    subGs = ordered_subgraphs(G, 'source')
    validate_subgraphs(G, subGs, 'source')
    execution_orders = [edge_order(g, 'source') for g in subGs]
    print(execution_orders)
