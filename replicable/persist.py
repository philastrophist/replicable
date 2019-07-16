from __future__ import print_function, unicode_literals, division, generators
from functools import wraps, partial
from operator import itemgetter
import operator
import networkx as nx
import numpy as np
import streamz
from tqdm import tqdm


from replicable import dependency

try:
    import itertools.imap as map
except ImportError:
    pass

try:
    import itertools.izip as zip
except ImportError:
    pass



def wrap_task(func, outnames, reader):
    """
    Return a function suitable for use with streams. The returned function will return [id, completed, results, Exception]
    :param func: function
    :return: function
    """
    @wraps(func)
    def inner(previous):
        id, completed, inputs, Exception = previous
        try:
            if all(o in completed for o in outnames):
                xs = reader(outnames)
            else:
                xs = func(*inputs)
        except Exception as e:
            return [(id, completed, None) for o in outnames], e
        return [(id, completed, x, None) for x in xs], None
    return inner


class DelayedStream(object):
    def __init__(self, index, name):
        super(DelayedStream, self).__init__()
        assert isinstance(index, PersistedSpecificationIndex)
        self.index = index
        self.name = name

    def comparison(self, o, function):
        name = '{}({}, {})'.format(function.__name__, self.name, o.name)
        self.index.add_task('comparison', 'per-object', function, name, [self.name, o.name], [name], [(bool, tuple())],
                            descriptions='')
        return DelayedStream(self.index, name)

    def __eq__(self, o):
        return self.comparison(o, operator.eq)

    def __ne__(self, o):
        return self.comparison(o, operator.ne)

    def __repr__(self):
        return super(DelayedStream, self).__repr__()

    def __and__(self, n):
        return self.comparison(n, operator.and_)

    def __or__(self, n):
        return self.comparison(n, operator.or_)

    def __xor__(self, n):
        return self.comparison(n, operator.xor)

    def __invert__(self):
        name = '{}({})'.format(operator.invert.__name__, self.name)
        self.index.add_task('comparison', 'per-object', operator.invert, name, [self.name], [name], [(bool, tuple())],
                            descriptions='')
        return DelayedStream(self.index, name)

    def __lt__(self, x):
        return self.comparison(x, operator.lt)

    def __le__(self, x):
        return self.comparison(x, operator.le)

    def __gt__(self, x):
        return self.comparison(x, operator.gt)

    def __ge__(self, x):
        return self.comparison(x, operator.ge)


class PersistedSpecificationIndex(object):
    """
    Framework for execution and persistence of results

    There are 3 actions available:
    .map:       Map a user defined function to user defined input data for each parameter set. This assumes that the
                function can act independently on each parameter set (embarrassingly parallel). Streamed
    .reduce:    Reduce the dataset by applying a user defined function on pairs of parameter sets. Streamed
    .aggregate: Batch execution by dask. The user defined function is given dask delayed input as input and stream
                execution is halted to allow the user to use all of the cluster for this task. Blocking.

    as well as the maintenance/utility functions of
    .assemble: Piece together the individual outputs into one file (useful for batch aggregation or user manipulation
    .filter:   Logical indexing of the parameter set to select a subset of results. Accessible by __getitem__

    When the user specifies a workflow using the context:
        >>> with spec:
        >>>     spec.map(function, ...)
        >>>     spec.reduce(function, ...)
        >>>     ...
    A dependency graph is built and partitioned to ensure that the user has full access to the cluster in `aggregate` calls.
    The graph is then analysed to produce an execution order that allows this.
    Streamz are constructed from that execution order and run.

    In each partition, data persists on the cluster until garbage collection by dask and streamz. However, with each
    partition, the data must be read again, since the cluster is cleared at that time to avoid memory errors.

    The flow of an action is this:
        1. Check that each output is not stored in a accessible stream, if so then return that stream and avoid repetition
        2. Read filenames from source
        3. If index is recorded as complete
            a: read result from individual file or assembly if available
           else:
            a. perform the function
            b. Write result and is_complete to individual file
            c. Write is_complete to index
            d. Write errors/stdout to log
        4. Store stream as accessible for this partition

    So there are 4 representations of task:
        1. User defined context
        2. Partitioned Graphs
        3. Partitioned Task order
        4. Executable `Streamz.Stream()`s

    When context is exited, execution begins

    """
    def __init__(self, directory, specification=None, seed=None, mode='a', client=None):
        """
        1. if specification is not supplied, read index to acquire it
        2. else, check compatibility of specification with the directory and seed
        3. Initialise pipeline
        :param directory: str
        :param specification: specification object or None
        :param seed: int or None
        :param mode: read mode, 'a' append is the default
        """
        self.directory = directory
        self.seed = seed
        self.mode = mode
        self.client = client
        if specification is None:
            self.specification = self.build_specification_from_index()
        else:
            self.specification = specification
            self.check_specfication_compatibility()


    def read_results(self, names):
        if not recorded_as_valid:
            raise AssertionError("Not recorded as valid")
        return (results, errors)

    def write_results(self, results, outnames, structures, descriptions):
        try:
            # record data
            # record attr that data was written correctly
            return (results, None)
        except Exception as e:
            return (results, e)


    def verify_function(self, function, name, innames, outnames, structures, store):
        """
        return whether the function, innames and outnames have been used before either in_stream (this session)
        or written to file (in_index).
        The match must be exact in function, names, and structures, otherwise it will raise and Exception
        if store is True, the function will be pickled on disk for future comparison
        if store is False, this function will not be compared against, all processes will run again to reproduce this data
        :return:
        """
        return


    def __enter__(self):
        self.clear()


    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        begin execution of dask pipeline now, upon closing context
        """
        self.partition_graph()
        self.build_streams()
        self.sink_streams()
        self.run_streams()
        self.clear()


    def sink_streams(self):
        for substream in self.substreams:
            streamz.zip(*substream['errors']).gather().sink(self.log_error)
            streamz.zip(*substream['results']).gather().sink(self.index_result)
            streamz.zip(*substream['assembles']).gather().sink(self.index_assemble)
            streamz.zip(*substream['aggregates']).gather().sink(self.index_aggregation)


    def run_streams(self):
        for substream in self.substreams:
            for param in tqdm(self.specification.iterate(), total=len(self.specification)):
                substream['source'].emit(param)


    def clear(self):
        self.graph = nx.DiGraph()
        self.graph.add_node('source', type='source')
        self.subgraphs = []
        self.execution_order = []
        self.substreams = []


    def add_task(self, action, node_type, function, name, innames, outnames, structures, descriptions, store=True):
        """
        Add map/reduce/aggregate/assemble etc to the spec
        """
        self.verify_function(node_type, function, name, innames, outnames, structures)  # (raise if incompatible)
        for outname in outnames:
            self.graph.add_node(outname, type=node_type)
            for inname in innames:
                if inname in self.specification.names:
                    src = 'source'
                else:
                    src = inname
                self.graph.add_edge(src, outname, action=action, name=name, function=function, structures=structures,
                                    descriptions=descriptions, outnames=outnames, innames=innames, store=store)


    def partition_graph(self):
        dependency.validate_graph(self.graph, 'source')
        self.subgraphs = dependency.ordered_subgraphs(self.graph, 'source')
        dependency.validate_subgraphs(self.graph, self.subgraphs, 'source')
        self.execution_order = [dependency.edge_order(g, 'source') for g in self.subgraphs]
        # remove tasks that are already completed
        # self.execution_order = [task for task in self.execution_order if any(not self.completed(o) for o in task['outnames'])]


    def build_streams(self):
        for stream, graph, chain in zip(self.substreams, self.subgraphs, self.execution_order):
            if chain[1]['type'] == 'agg':
                stream['source'] = streamz.Source()
                # TODO: implement aggregate calls
            else:
                stream['source'] = streamz.Source().scatter()
                stream['id'] = stream['source'].pluck('id')
                for name in self.specification.names:  # separate parameters for easy handling
                    stream['results'][name] = stream['source'].pluck(name)
                for task in chain:
                    self.construct_stream_task(stream, graph, task)


    def construct_stream_task(self, stream, graph, task):
        inputs = [stream['results'][i] for i in task['innames']]
        function = wrap_task(task['function'], task['outnames'], partial(self.read_results, task['outnames']))
        # this now handles errors and reading completed results
        result = task['action'](inputs, function)

        output, error = result.pluck(-2), result.pluck(-1)
        for i, (outname, struct, desc) in enumerate(zip(task['outnames'], task['structures'], task['descriptions'])):
            stream['results'][outname] = output.pluck(i) # make available to other functions in the same partition
            if task['store']:
                stream['writes'].append([outname, stream['results'][outname], struct, desc])  # queue for writing to index and individual files
        stream['errors'].append(error)

    def construct_map(self, inputs, function):
        return streamz.zip(*inputs).map(function)

    def construct_reduce(self, inputs, function):
        return streamz.zip(*inputs).accumulate(function)


    def __getitem__(self, item):
        """
        Three cases:
        * Item is a Parameter/representation (which are held in index, in memory)
            >>> spec['param1']  # returns direct read from index (held in memory) - pd.Series indexed with filenames
        * Item is a result key (held in individual files)
            >>> spec['result1']  # returns read from files (filenames given by spec)
        *Item is a boolean index mask generated from the above cases
            `spec[(spec['a'] > 0) & (spec['b'] < 0)]` equates to:
            >>> source = Stream().scatter()
            >>> filt1 = source.construct_map(lambda s: s['a']).construct_map(lambda x: x > 0)
            >>> filt2 = source.construct_map(lambda s: s['b']).construct_map(lambda x: x < 0)
            >>> indexed = filt1.zip(filt2).construct_map(lambda x: and_(*x)).zip(source).filter(itemgetter(0)).pluck(1)
            >>> indexed.buffer(nworkers*2).gather().sink(print)

        returns a DelayedStream() which is thin wrapper around a streamz object
        """
        if isinstance(item, DelayedStream):
            name =  'filter-{}'.format(item.name)
            self.filter(lambda x: x, name, [item.name])
        elif isinstance(item, str):
            return DelayedStream(self, item)
        else:
            raise TypeError("Item must be either a name of a variable in the graph or a stream instance")


    def map(self, function, name, innames, outnames, structures, descriptions):
        self.add_task('map', 'per-object', function, name, innames, outnames, structures, descriptions)


    def filter(self, function, name, innames, store=False, description=''):
        if store:
            outnames = [name]
            structures = [(bool, 1)]
        else:
            outnames = []
            structures = []
        self.add_task('filter', 'per-object', function, name, innames, outnames, structures, descriptions=[description])


    def reduce(self, function, name, innames, outnames, structures, descriptions):
        self.add_task('reduce', 'reduction', function, name, innames, outnames, structures, descriptions)


    def assemble(self, *keys):
        """
        Copy `key` result from individual files to an aggregation file containing all results!
        :param key:
        :return:
        """
        for key in keys:
            self.reduce(self.write_assemble_reduction, 'assemble-{}'.format(key), keys, [], [], [])


    def aggregate(self, function, name, innames, outnames, structures, descriptions):
        """
        Store the result of a `function` which acts on the results from many individual parameter sets.
        e.g. building a histogram requires `aggregate` since it requires all results (technically better to use reduce in this case though)
        >>> spec.aggregate(np.hist, 'histogram', ['result1'], ['bins', 'count'])
        :param function:
        :param innames:
        :param outnames:
        :param structures:
        :param descriptions:
        :return:
        """
        self.add_task('aggregate', 'agg', function, name, innames, outnames, structures, descriptions)