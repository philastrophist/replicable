from __future__ import print_function, unicode_literals, division, generators
import os
from itertools import product

import dask
import dask.bag as db
import dask.dataframe as dd
import h5py
import pandas as pd
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


class Parameter(object):
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


class Constant(Parameter):
    def __init__(self, names, values):
        super(Constant, self).__init__(names)
        self.values = np.atleast_1d(values)

    @property
    def shape(self):
        return self.values.shape


class Stochastic(Parameter):
    def __init__(self, names, sampler, n):
        super(Stochastic, self).__init__(names)
        self.sampler = sampler
        self.n = n

    @property
    def shape(self):
        return self.n,

    def sample(self, n=1):
        yield {name: values for name, values in zip(self.names, self.sampler(n))}


class IntegrityError(Exception):
    pass


class Specification(object):
    def __init__(self, *parameters):
        self.directory = None
        self.seed = None

        self.parameters = parameters
        self.gridded = [p for p in self.parameters if isinstance(p, Constant)]
        self.stochastic = [p for p in self.parameters if isinstance(p, Stochastic)]
        self.unpacked_gridded = [(name, value) for p in self.gridded for name, value in p.items()]
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

    def __call__(self, directory, seed):
        """use a directory for storing simulations together with a seed to create them"""
        self.directory = directory
        self.seed = seed

    def __enter__(self):
        self.index_fname = os.path.join(self.directory, 'index-{}.h5'.format(self.hash_name))
        if not os.path.exists(self.index_fname):
            self.overwrite_index()
        else:
            self.validate_integrity(verbose=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.directory, self.seed = None, None

    def overwrite_index(self):
        with h5py.File(self.index_fname, 'w', libver='latest') as f:
            pass
        store = pd.HDFStore(self.index_fname, 'r+')
        for paramset, hsh in tqdm(self.iterate(), total=self.size, desc='building index'):
            df = pd.DataFrame(paramset)
            df['hash'] = hsh.hexdigest()
            store.append('index', df, format='table',  data_columns=True)

    @property
    def files(self):
        _files = []
        for root, dirs, _files in os.walk(self.directory):
            pass
        _files = [os.path.join(self.directory, f) for f in _files if f != self.index_fname]
        return _files

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



    def read_parameter(self, parameter):
        with h5py.File(self.index_fname, 'r') as f:
            return f['parameters'][parameter]


    def validate_integrity(self, verbose=True):
        """
        Validates the integrity of the index:
        Are all files present?
        Does the total hash for the files match that which is expected by the specification?
        :return: True if valid
        """
        nmissing = len(self) - len(self.files)
        if nmissing > 0:
            raise IntegrityError("Missing {} files, run `integrity_audit` to identify them".format(nmissing))
        elif nmissing < 0:
            raise IntegrityError("There are {} more valid files than were expected, run `integrity_audit` "
                                 "to identify them.".format(-nmissing))
        hsh = xxhash.xxh64()
        for f in tqdm(self.files, desc='Hashing files', disable=not verbose):
            hsh.update(os.path.basename(f).strip('.h5'))
        file_hash = hsh.hexdigest()
        hsh = xxhash.xxh64()
        for paramset, hsh in tqdm(self.iterate(), total=self.size, desc='Hashing parameters', disable=not verbose):
            hsh.update(hsh)
        param_hash = hsh.hexdigest()
        if file_hash != param_hash:
            raise IntegrityError("Hash mismatch: files are corrupted or mislabelled, run `integrity_audit` to identify"
                                 "the problematic ones")
        return True

    def integrity_audit(self, test_existence=True, test_read=False, verbose=True):
        missing = []
        for i, (paramset, hash) in enumerate(tqdm(self.iterate(), total=self.size, desc='Hashing parameters',
                                                  disable=not verbose)):
            fname = os.path.join(self.directory, '{}.h5')
            if test_existence:
                if not os.path.exists(fname):
                    missing.append((i, hash))

    def save(self, results, outnames, params, param_hash):
        """
        Save results from a function mapped to a simulation dataset
        :param results: The list of outputs from the function
        :param outnames: Names for each output in the results list
        :param params: The simulation parameters used to create the results
        :param param_hash: The hash of the parameters
        :return:
        """
        h = param_hash.hexdigest()
        fname = os.path.join(self.directory, h+'.h5')
        with h5py.File(fname, 'a', libver='latest') as f:
            f.attrs['hash'] = h
            parameters = f.require_group('parameters')
            outputs = f.require_group('output')
            for key, value in params.items():
                parameters.require_dataset(key, value.shape, value.dtype, exact=True)
            for result, outname in zip(results, outnames):
                outputs.require_dataset(outname, dtype=result.dtype, shape=result.shape, exact=True, data=result)

    def iterate(self):
        names, ranges = zip(*self.unpacked_gridded)
        prod = product(*ranges)
        griddeds = ({n: p for n, p in zip(names, ps)} for ps in prod)
        with state(self.seed):
            iterators = [p.sample(1) for p in self.stochastic] + [griddeds]
            while True:
                parameters = reduce(lambda a, b: a.update(b), map(next, iterators))
                parameters['random_seed'] = self.seed
                yield parameters, dict_hash(parameters)

    def map(self, function, outnames, verbose=True):
        for paramset, hsh in tqdm(self.iterate(), total=self.size, disable=not verbose):
            results = function(**paramset)
            assert len(results) == len(outnames), "Length of `outnames` must be the same as length of function output"
            self.save(results, outnames, paramset, hsh)

    def read(self, fnames):
        return [dask.delayed(partial(load, key=key))(fname) for fname in fnames]

    def __getitem__(self, item):
        df = dd.read_hdf(self.index_fname, 'index')
        if item in self.names:
            return df[item]
        return self.read(df['hash'] + '.h5')


class DelayedStream(object):
    def __init__(self, streamz_object):
        self.streamz_object = streamz_object

    def evalutate(self):
        pass



class PersistedSpecification(object):
    def __init__(self, directory, specification=None, seed=None, mode='a'):
        """
        :param directory: str
        :param specification: specification object or None
        :param seed: int or None
        :param mode: read mode, 'a' append is the default
        """
        self.specification = specification
        self.directory = directory
        self.seed = seed
        self.mode = mode

    def __enter__(self):
        """
        1. if specification is not supplied, read index to acquire it
        2. else, check compatibility of specification with the directory and seed
        3. Initialise pipeline
        """

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        begin execution of dask pipeline now, upon closing context
        """
        self.source.gather().sink(self.sink)  # sink being the function to store all the data
        for param in self.specification.iterate():
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
            >>> indexed = filt1.zip(filt2).map(lambda x: and_(*x)).zip(source).filter(lambda x: x[0]).map(lambda x: x[1])
            >>> indexed.buffer(nworkers*2).gather().sink(print)

        returns a DelayedStream() which is thin wrapper around a streamz object
        """
        return DelayedStream()


    def assemble(self, *keys):
        """
        Copy `key` result from individual files to an aggregation file containing all results!
        :param key:
        :return:
        """
        for key in keys:
            self.aggregate_maps[key] = self.result_maps[key].partition(npartitions).map(self.write_aggregates, key=key)


    def map(self, function, name, innames, outnames, structures, descriptions):
        m = self.source.map(function).map(self.write_results, keys=outnames, structures=structures, descriptions=descriptions)
        self.result_maps[]


    def aggregate(self, function, name, innames, outnames, structures, descriptions):
        """
        Store the result of a `function` which acts on the results from many individual parameter sets.
        e.g. building a histogram requires `aggregate` since it requires all results
        >>> spec.aggregate(np.hist, 'histogram', ['result1'], ['bins', 'count'])
        :param function:
        :param innames:
        :param outnames:
        :param structures:
        :param descriptions:
        :return:
        """