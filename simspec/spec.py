import os
from itertools import product
import h5py
import pandas as pd
import contextlib
import numpy as np
import xxhash
from tqdm import tqdm


@contextlib.contextmanager
def state(seed):
    state = np.random.get_state()
    np.random.seed(seed)
    try:
        yield
    finally:
        np.random.set_state(state)

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
        return (len(self.names), )

    @property
    def size(self):
        return np.prod(*self.shape)


class Gridded(Parameter):
    def __init__(self, names, values):
        super(Gridded, self).__init__(names)
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
        return (self.n, )

    def sample(self, n=1):
        yield {name: values for name, values in zip(self.names, self.sampler(n))}



class Specification:
    def __init__(self, *parameters):
        self.directory = None
        self.seed = None

        self.parameters = parameters
        self.gridded = [p for p in self.parameters if isinstance(p, Gridded)]
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
        self.directory = directory
        self.seed = seed


    def __enter__(self):
        self.index_fname = os.path.join(self.directory, 'index-{}.h5'.format(self.hash_name))
        if not os.path.exists(self.index_fname):
            self.overwrite_index()
        else:
            self.validate_index()
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.directory, self.seed = None, None


    def overwrite_index(self):
        with h5py.File(self.index_fname, 'w', libver='latest'):
            pass
        store = pd.HDFStore(self.index_fname, 'r+')
        for paramset, hash in tqdm(self.iterate(0), total=self.size, desc='building index'):
            df = pd.DataFrame(paramset)
            df['hash'] = hash.hexdigest()
            store.append('index', df, format='t',  data_columns=True)


    def create_virtual_link(self, parameter):
        for root, dirs, files in os.walk(self.directory):
            files = [f for f in files if f != self.index_fname]
        if len(files) == 0:
            return
        first = files[0][parameter]
        with h5py.File(os.path.join(self.directory, first), 'r') as first:
            layout = h5py.VirtualLayout(shape=(self.size, )+first.shape, dtype=first.dtype)

        for i, file in enumerate(files):
            vsource = h5py.VirtualSource(file, parameter, shape=first.shape, dtype=first.dtype)
            layout[i] = vsource

        # Add virtual dataset to output file
        with h5py.File(self.index_fname, 'a', libver='latest') as f:
            f.create_virtual_dataset('data', layout, fillvalue=np.nan)

    def validate_index(self):
        pass


    @classmethod
    def from_index(cls, directory):
        pass


    def save(self, results, outnames, params, hash):
        fname = os.path.join(self.directory, hash.hexdigest()+'.h5')
        with h5py.File(fname, 'a', libver='latest') as f:
            for result, outname in zip(results, outnames):
                f.require_dataset(outname, dtype=result.dtype, shape=result.shape, exact=True, data=result)


    def run(self, function, outnames, seed, verbose=True):
        for paramset, hash in tqdm(self.iterate(seed), total=self.size, disable=not verbose):
            results = function(**paramset)
            self.save(results, outnames, paramset, hash)


    def iterate(self, seed):
        names, ranges = zip(*self.unpacked_gridded)
        prod = product(*ranges)
        griddeds = ({n: p for n, p in zip(names, ps)} for ps in prod)
        with state(seed):
            iterators = [p.sample(1) for p in self.stochastic] + [griddeds]
            while True:
                parameters = reduce(lambda a, b: a.update(b), map(next, iterators))
                yield parameters, dict_hash(parameters)


