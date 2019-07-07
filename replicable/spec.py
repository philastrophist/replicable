from __future__ import print_function, unicode_literals, division, generators
import os
from functools import wraps
from itertools import product
from operator import itemgetter

import dask
import dask.bag as db
import dask.dataframe as dd
import h5py
import pandas as pd
import streamz
import contextlib
import numpy as np
import xxhash
from tqdm import tqdm

try:
    import itertools.imap as map
except ImportError:
    pass

try:
    import itertools.izip as map
except ImportError:
    pass


@contextlib.contextmanager
def state(seed):
    rng_state = np.random.get_state()
    np.random.seed(seed)
    try:
        yield
    finally:
        np.random.set_state(rng_state)


def dict_hash(d):
    h = xxhash.xxh64()
    stuff = sorted(''.join(list(map(str, d.keys())) + list(map(str, d.values()))))
    h.update(stuff)
    return h

class Parameters(dict):
    @property
    def hash(self):
        return dict_hash(self)


class Variable(object):
    def __init__(self, names):
        self.names = names

    def __add__(self, other):
        return Specification(self, other)

    @property
    def shape(self):
        return len(self.names),

    @property
    def size(self):
        return np.prod(*self.shape)

    def iterate(self, seed=0):
        return Specification(self).iterate(seed)


class Constant(Variable):
    def __init__(self, names, values):
        super(Constant, self).__init__(names)
        self.values = np.atleast_1d(values)

    def __repr__(self):
        return "<ConstantParameter({})>".format({n:v for n,v in zip(self.names, self.values)})

    def __getitem__(self, item):
        try:
            return self.values[self.names.index(item)]
        except ValueError:
            raise KeyError("key {} is not present".format(item))

    def __eq__(self, other):
        if self.size != other.size:
            return False
        try:
            return all((other[i] == self[i]) and (type(other[i]) == type(self[i])) for i in self.names)
        except KeyError:
            return False


    @property
    def shape(self):
        return self.values.shape


class Stochastic(Variable):
    def __init__(self, names, sampler, n):
        super(Stochastic, self).__init__(names)
        self.sampler = sampler
        self.n = n

    @property
    def shape(self):
        return self.n,

    def sample(self, rng, n=1):
        yield {name: values for name, values in zip(self.names, self.sampler(rng, n))}


class IntegrityError(Exception):
    pass


class Specification(object):
    def __init__(self, *parameters):
        self.parameters = parameters
        self.gridded = [p for p in self.parameters if isinstance(p, Constant)]
        self.stochastic = [p for p in self.parameters if isinstance(p, Stochastic)]
        self.unpacked_gridded = [(name, value) for p in self.gridded for name, value in zip(p.names, p.values)]
        assert len(set(self.names)) == len(self.names), "Unique parameter names must be used"

    @property
    def names(self):
        return [p for params in self.parameters for p in params.names]

    @property
    def size(self):
        return np.prod([p.size for p in self.parameters])

    def __len__(self):
        return self.size

    @property
    def shape(self):
        return reduce(lambda a, b: a + b, [p.shape for p in self.parameters])

    def __call__(self, directory, seed, mode='r'):
        """use a directory for storing simulations together with a seed to create them"""
        return PersistedSpecification(directory, self, seed, mode)


    def iterate(self, seed=0):
        """
        Iterate over all parameters
        :param seed: int: Seed for stochastic components
        :return: generator
        """
        rng = np.random.RandomState(seed)
        names, ranges = zip(*self.unpacked_gridded)
        prod = product(*ranges)
        griddeds = ({n: p for n, p in zip(names, ps)} for ps in prod)
        iterators = [p.sample(rng, 1) for p in self.stochastic] + [griddeds]
        while True:
            parameters = reduce(lambda a, b: a.update(b), map(next, iterators))
            yield Parameters(**parameters)

    # def __enter__(self):
    #     self.index_fname = os.path.join(self.directory, 'index-{}.h5'.format(self.hash_name))
    #     if not os.path.exists(self.index_fname):
    #         self.overwrite_index()
    #     else:
    #         self.validate_integrity(verbose=True)
    #     return self
    #
    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     self.directory, self.seed = None, None

    # def overwrite_index(self):
    #     with h5py.File(self.index_fname, 'w', libver='latest') as f:
    #         pass
    #     store = pd.HDFStore(self.index_fname, 'r+')
    #     for paramset, hsh in tqdm(self.iterate(), total=self.size, desc='building index'):
    #         df = pd.DataFrame(paramset)
    #         df['hash'] = hsh.hexdigest()
    #         store.append('index', df, format='table',  data_columns=True)

    # @property
    # def files(self):
    #     _files = []
    #     for root, dirs, _files in os.walk(self.directory):
    #         pass
    #     _files = [os.path.join(self.directory, f) for f in _files if f != self.index_fname]
    #     return _files

    # def _create_virtual_link(self, dataset_names, verbose=True):
    #     parameter_generator = tqdm(self._iterate(), total=self.size, dsec='linking', disable=not verbose)
    #     first_fname = next(parameter_generator)[0]
    #     with h5py.File(os.path.join(self.directory, first_fname), 'r') as first:
    #         layouts = [h5py.VirtualLayout(shape=(self.size, )+first[ds].shape, dtype=first[ds].dtype) for ds in dataset_names]
    #
    #     for i, (file, hash) in enumerate(parameter_generator):
    #         vsources = [h5py.VirtualSource(file, ds, shape=first.shape, dtype=first.dtype)]
    #         layout[i] = vsource
    #
    #     # Add virtual dataset to output file
    #     with h5py.File(self.index_fname, 'a', libver='latest') as f:
    #         f.create_virtual_dataset('data', layout, fillvalue=np.nan)


    #
    # def read_parameter(self, parameter):
    #     with h5py.File(self.index_fname, 'r') as f:
    #         return f['parameters'][parameter]

    #
    # def validate_integrity(self, verbose=True):
    #     """
    #     Validates the integrity of the index:
    #     Are all files present?
    #     Does the total hash for the files match that which is expected by the specification?
    #     :return: True if valid
    #     """
    #     nmissing = len(self) - len(self.files)
    #     if nmissing > 0:
    #         raise IntegrityError("Missing {} files, run `integrity_audit` to identify them".format(nmissing))
    #     elif nmissing < 0:
    #         raise IntegrityError("There are {} more valid files than were expected, run `integrity_audit` "
    #                              "to identify them.".format(-nmissing))
    #     hsh = xxhash.xxh64()
    #     for f in tqdm(self.files, desc='Hashing files', disable=not verbose):
    #         hsh.update(os.path.basename(f).strip('.h5'))
    #     file_hash = hsh.hexdigest()
    #     hsh = xxhash.xxh64()
    #     for paramset, hsh in tqdm(self.iterate(), total=self.size, desc='Hashing parameters', disable=not verbose):
    #         hsh.update(hsh)
    #     param_hash = hsh.hexdigest()
    #     if file_hash != param_hash:
    #         raise IntegrityError("Hash mismatch: files are corrupted or mislabelled, run `integrity_audit` to identify"
    #                              "the problematic ones")
    #     return True

    # def integrity_audit(self, test_existence=True, test_read=False, verbose=True):
    #     missing = []
    #     for i, (paramset, hash) in enumerate(tqdm(self.iterate(), total=self.size, desc='Hashing parameters',
    #                                               disable=not verbose)):
    #         fname = os.path.join(self.directory, '{}.h5')
    #         if test_existence:
    #             if not os.path.exists(fname):
    #                 missing.append((i, hash))

    # def save(self, results, outnames, params, param_hash):
    #     """
    #     Save results from a function mapped to a simulation dataset
    #     :param results: The list of outputs from the function
    #     :param outnames: Names for each output in the results list
    #     :param params: The simulation parameters used to create the results
    #     :param param_hash: The hash of the parameters
    #     :return:
    #     """
    #     h = param_hash.hexdigest()
    #     fname = os.path.join(self.directory, h+'.h5')
    #     with h5py.File(fname, 'a', libver='latest') as f:
    #         f.attrs['hash'] = h
    #         parameters = f.require_group('parameters')
    #         outputs = f.require_group('output')
    #         for key, value in params.items():
    #             parameters.require_dataset(key, value.shape, value.dtype, exact=True)
    #         for result, outname in zip(results, outnames):
    #             outputs.require_dataset(outname, dtype=result.dtype, shape=result.shape, exact=True, data=result)



    # def map(self, function, outnames, verbose=True):
    #     for paramset, hsh in tqdm(self.iterate(), total=self.size, disable=not verbose):
    #         results = function(**paramset)
    #         assert len(results) == len(outnames), "Length of `outnames` must be the same as length of function output"
    #         self.save(results, outnames, paramset, hsh)


def map_wrap(func):
    """
    Return a function suitable for use with streams. The returned function will return [result, [Exception]]
    :param func: function
    :return: function
    """
    @wraps(func)
    def inner(previous, *args, **kwargs):
        if isinstance(previous[1], Exception):
            return previous
        try:
            x = func(previous[0], *args, **kwargs)
        except Exception as e:
            return None, e
        return x, None
    return inner


class DelayedStream(object):
    def __init__(self, stream, name):
        super(DelayedStream, self).__init__()
        self.stream = stream
        self.name = name

    def to_stream(self):
        return self.stream

    def to_dask_futures(self):
        raise NotImplementedError("Extracting dask futures not yet supported")

    def __eq__(self, o):
        return super(DelayedStream, self).__eq__(o)

    def __ne__(self, o):
        return super(DelayedStream, self).__ne__(o)

    def __repr__(self):
        return super(DelayedStream, self).__repr__()

    def __and__(self, n):
        return super(DelayedStream, self).__and__(n)

    def __or__(self, n):
        return super(DelayedStream, self).__or__(n)

    def __xor__(self, n):
        return super(DelayedStream, self).__xor__(n)

    def __invert__(self):
        return super(DelayedStream, self).__invert__()

    def __lt__(self, x):
        return super(DelayedStream, self).__lt__(x)

    def __le__(self, x):
        return super(DelayedStream, self).__le__(x)

    def __gt__(self, x):
        return super(DelayedStream, self).__gt__(x)

    def __ge__(self, x):
        return super(DelayedStream, self).__ge__(x)


class MaskStream(DelayedStream):
    pass


class PersistedSpecification(object):
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

        self.result_streams = []
        self.error_streams = []
        self.assemble_streams = []
        self.aggregate_streams = []


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


    def verify_function(self, function, innames, outnames, structures):
        """
        return whether the function, innames and outnames have been used before either in_stream (this session)
        or written to file (in_index).
        The match must be exact in function, names, and structures, otherwise it will raise and Exception
        :param innames:
        :param outnames:
        :param structures:
        :return:
        """
        return in_stream, in_index

    def __enter__(self):
        self.result_streams = []
        self.error_streams = []
        self.assemble_streams = []
        self.aggregate_streams = []


    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        begin execution of dask pipeline now, upon closing context
        """
        errors = streamz.zip(*self.error_streams).gather().sink(self.log_error)
        results = streamz.zip(*self.result_streams).gather().sink(self.index_result)
        assembles = streamz.zip(*self.assemble_streams).gather().sink(self.index_assemble)
        aggregations = streamz.zip(*self.aggregate_streams).gather().sink(self.index_aggregation)
        for param in tqdm(self.specification.iterate(), total=len(self.specification)):
            self.source.emit(param)
        self.source = None


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
            >>> filt1 = source.map(lambda s: s['a']).map(lambda x: x > 0)
            >>> filt2 = source.map(lambda s: s['b']).map(lambda x: x < 0)
            >>> indexed = filt1.zip(filt2).map(lambda x: and_(*x)).zip(source).filter(itemgetter(0)).pluck(1)
            >>> indexed.buffer(nworkers*2).gather().sink(print)

        returns a DelayedStream() which is thin wrapper around a streamz object
        """
        if isinstance(item, MaskStream):
            raise NotImplementedError("Filtering by boolean mask is not yet supported")
        elif item in self.assemble_streams:
            return self.assemble_streams[item]
        elif item in self.result_streams:
            return self.result_streams[item]
        elif item in self.aggregate_streams:
            return self.aggregate_streams[item]
        elif item in self.specification.names:
            return DelayedStream(map_wrap(self.source.map(itemgetter(item))))
        else:
            raise KeyError("{} is not a result of a mapping/reduction/aggregation or a parameter")


    def assemble(self, *keys):
        """
        Copy `key` result from individual files to an aggregation file containing all results!
        :param key:
        :return:
        """
        for key in keys:
            self.aggregate_maps[key] = self.result_maps[key].partition(npartitions).map(self.write_aggregates, key=key)


    def map(self, function, name, innames, outnames, structures, descriptions):
        outnames = ['{}/{}'.format(name, o) for o in outnames]
        in_stream, in_index = self.verify_function(function, innames, outnames, structures)  # make sure its the same one as used before (raise if incompatible)
        function = map_wrap(function)  # map_wrap handles errors, returning in form (results, errors[=None])

        if not in_stream:
            if not in_index:
                result_and_errors = self.source.map(function)
                result_and_errors.map(self.write_results, outnames=outnames, structures=structures, descriptions=descriptions)
            else:
                result_and_errors = self.source.map(self.read_results, names=outnames)

            result, error = result_and_errors.pluck(0), result_and_errors.pluck(1)
            for i, name in enumerate(outnames):
                self.result_streams[name] = DelayedStream(result.pluck(i), name)
            self.error_streams.append(error)  # record any errors using one process
        return tuple([self.result_streams[i] for i in outnames])



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
        outnames = ['{}/{}'.format(name, o) for o in outnames]


    def reduce(self, function, name, innames, outnames, structures, descriptions):
        outnames = ['{}/{}'.format(name, o) for o in outnames]
        raise NotImplementedError("Reduce is not yet implemented")