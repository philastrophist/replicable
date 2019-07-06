import pytest
from itertools import product, permutations
from functools import reduce
import numpy as np
try:
    import itertools.imap as map
except ImportError:
    pass

try:
    import itertools.izip as map
except ImportError:
    pass


from simspec.spec import Constant, Stochastic

def test_Constant_iterate_produces_parameters():
    c = Constant('a', [1, 2])
    result = c.iterate()
    assert isinstance(result, Parameters)
    assert isinstance(result['a'], int)


@pytest.mark.parametrize(['a', 'b', 'evaluate_to'], [('b', [1, 2]), ('b', [1, 2]), True])
@pytest.mark.parametrize(['a', 'b', 'evaluate_to'], [('b', [2, 2]), ('b', [1, 2]), False])
@pytest.mark.parametrize(['a', 'b', 'evaluate_to'], [('b', [1., 2]), ('b', [1, 2]), False]) # dtype
@pytest.mark.parametrize(['a', 'b', 'evaluate_to'], [('a', [1, 2]), ('b', [1, 2]), False])
def test_parameters_equality_comparison(a, b, evaluate_to):
    a = Constant(*a)
    b = Constant(*b)
    assert (a == b) is evaluate_to

@pytest.mark.parametrize('seed', [0, 4, 14524])
def test_stochastic_generates_random(seed):
    def sampler(rng, size):
        return rng.uniform(0, 1, size=size)
    c = Stochastic('a', sampler, 10)
    expected = np.random.RandomState(seed).uniform(0, 1, size=10)
    output = np.array([i['a'] for i in c.iterate(seed)])
    np.testing.assert_allclose(output, expected)


def test_Stochasticiterate_produces_parameters():
    def sampler(rng, size):
        return rng.uniform(0, 1, size=size)
    c = Stochastic('a', sampler, 10)
    result = c.iterate(0)
    assert isinstance(result, Parameters)
    assert isinstance(result['a'], float)


def test_Constant_accepts_single():
    assert (i['a'] for i in Constant('a', [1]).iterate()) == (1,)
    assert (i['a'] for i in Constant(['a'], [1]).iterate()) == (1,)

def test_Constant_accepts_groups():
    assert ((i['a'], i['b']) for i in Constant(['a', 'b'], [[1, 2], [3, 4]]).iterate()) == ((1, 2), (3, 4))


@pytest.mark.parametrize('a', ([1, 2], [1]))
@pytest.mark.parametrize('b', ([2, 4], [2]))
def test_Constant_rejects_unmatched_groups(a, b):
    if len(a) == len(b):
        pass  # not testing same lengths
    with pytest.raises(ValueError):
        assert Constant(['a', 'b'], [a, b])


def test_Constant_generates_the_input():
    input = (0, 1, 2)
    c = Constant('a', input)
    combos = (i['a'] for i in c.iterate())
    assert combos == input

def test_Constant_addition_produces_a_grid():
    inputs = (0, 1, 2), (2, 4, 6)
    outputs = product(*inputs)
    a = Constant('a', input)
    b = Constant(['b'], input)
    c = a + b
    combos = ((i['a'], i['b']) for i in  c.iterate())
    assert combos == outputs


def test_single_and_pair_addition_produces_a_grid():
    singles = (0, 1), (2, 4)
    pairs = [(2, 4), (6, 8)]
    outputs = tuple(product(product(*singles), pairs))
    length = 8
    assert length == len(outputs)
    a = Constant('a', singles[0])
    b = Constant('b', singles[1])
    c = Constant(['c', 'd'], pairs)
    combo = a + b + c
    combos = ((i['a'], i['b'], i['c'], i['d']) for i in combo.iterate())
    assert combos == outputs

def test_single_pair_and_single_stochastic_addition_produces_a_grid():
    singles = (0, 1), (2, 4)
    pairs = [(2, 4), (6, 8)]

    def sampler(rng, size):
        return rng.uniform(0, 1, size=size)
    seed = 1
    stoch = Stochastic(['e'], sampler, 3)

    stoch_values = (i['e'] for i in stoch.iterate(seed))
    outputs = tuple(product(product(*singles), pairs, stoch_values))
    length = 24
    assert length == len(outputs)
    a = Constant('a', singles[0])
    b = Constant('b', singles[1])
    c = Constant(['c', 'd'], pairs)
    combo = a + b + c + stoch

    combos = ((i['a'], i['b'], i['c'], i['d']) for i in combo.iterate())
    assert combos == outputs

@pytest.mark.parametrize('seed', 1)
def test_stochastic_is_seeded(seed):
    def sampler(rng, size):
        return rng.uniform(0, 1, size=size)
    first = (i['a'] for i in Stochastic(['a'], sampler, 10).iterate(seed))
    second = (i['a'] for i in Stochastic(['a'], sampler, 10).iterate(seed))
    assert first == second


@pytest.mark.parametrize('seed', 1)
def test_stochastic_is_seeded_in_specification(seed):
    def sampler(rng, size):
        return rng.uniform(0, 1, size=size)

    specs = []
    for i in range(2):
        const1 = Constant('a', input)
        const2 = Constant(['b', 'c'], [[1, 2], [3, 4]])
        stoch = Stochastic(['d'], sampler, 10)
        specs.append(const1 + const2 + stoch)
    assert ((i['a'], i['b'], i['c'], i['d']) for i in specs[0].iterate()) == \
           ((i['a'], i['b'], i['c'], i['d']) for i in specs[1].iterate())

def test_param_order_unaffected_by_input_order():
    def sampler(rng, size):
        return rng.uniform(0, 1, size=size)
    combo = permutations([Constant('a', [1, 2, 3]), Constant(['b', 'c'], [[1, 2], [3, 4]]), Stochastic(['d'], sampler, 10)], 3)
    results = []
    for c in combo:
        results.append(((i['a'], i['b'], i['c'], i['d']) for i in c.iterate(0)))
    assert reduce(lambda a, b: a == b, results)


def test_hash_is_associative_with_spec_addition():
    """
    Ensure that hash for a parameter set is the same regardless of order of input
    """
    def sampler(rng, size):
        return rng.uniform(0, 1, size=size)
    options1 = [Constant('a', [1, 2, 3]), Constant(['b', 'c'], [[1, 2], [3, 4]]), Stochastic(['d'], sampler, 10)]

    options1 += options1[::-1]
    combo = permutations(options1, 3)
    results = []
    for c in combo:
        results.append(c.iterate(0).hash)
    assert reduce(lambda a, b: a == b, results)


