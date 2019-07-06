import os

import dask
import pytest

from simspec.spec import Constant, Stochastic
import numpy as np


def sampler(rng, size):
    return rng.uniform(0, 1, size=size)

@pytest.fixture
def specification():
    const1 = Constant('a', [1., 10.])
    const2 = Constant(['b', 'c'], [[1, 2], [3, 4]])
    stoch = Stochastic(['d'], sampler, 10)
    return const1 + const2 + stoch


def build_test_data(a, b, c, d):
    return np.linspace(a, b, 1000) * c / d


def duplicate_and_stack(data):
    return np.stack([data, data])


@pytest.fixture
def spec(specification, tmpdir):
    with specification(tmpdir, seed=0) as spec:
        spec.map(build_test_data, ['data'], args=['a', 'b', 'c', 'd'], shape=[(1000,)], dtype=float, desc='some test data')
        spec.map(duplicate_and_stack, ['stacked'], kwargs={'data':'data'}, shape=[(2, 1000)], dtype=float, desc='stacked data')
        spec.aggregate('stacked')
    return spec


def test_structure(spec):
    """
    Make sure output directory is:
        project/
            {environment_hash}-{source_hash}/
                {param1-hash}.h5
    """



def test_index_on_result_name_returns_delayed_read_in_different_context_to_the_execution_context(spec):
    """
    Ensure that indexing with `spec['result_of_map']` returns delayed that reads the files when outside of the context
    which produced that result.
    This should contrasted with behaviour in the same context (should return dask Futures directly from the map)
    """
    with spec:
        assert isinstance(spec['stacked'], dask.delayed.Delayed)
        spec['stacked'][]
    for f in spec.filenames:
        os.remove(f)  # test for error when trying to read
    with spec:
        assert isinstance(spec['stacked'], dask.delayed.Delayed)



def test_index_on_param_name_returns_delayed_array_from_direct_read_of_index_file():
    """
    Ensure that indexing with `spec['param1']` always returns a delayed dataframe which was directly read from the index
    file in one go (the index file should be local RAM-manageable
    """


def test_index_on_result_name_returns_delayed_which_waits_for_files_in_same_context_as_the_execution_context():
    """
    Ensure that indexing with `spec['result_of_map']` returns dask.Futures when insside the context which produced that
    result.
    This should contrasted with behaviour in a different context (should return delayed read)
    """


def test_map_store_and_read_back():
    """
    Ensure written map_data and metadata is correct.
    :return:
    """


def test_aggregate_store_and_read_back():
    """
    Test that aggregation stores all data for a result and it is read back correctly
    :return:
    """

def test_aggregate_reads_back_from_aggregate_file_not_individual_ones():
    """
    Test that aggregated data is read back from the aggregation file not the individual ones
    :return:
    """

def test_deletion_cancels_inprogress():
    pass

def test_deletion_removes_results_from_each_file_leaving_everything_else():
    pass


def test_deletion_removes_result_aggregation_file_leaving_everything_else():
    pass


def test_disallow_duplicate_names():
    """
    Ensure that duplicate names are detected before ANY processing
    :return:
    """

def test_only_allow_scalar_parameter_input():
    """
    Ensure that duplicate names are detected before ANY processing
    :return:
    """

def test_error_in_map_cancels_all_others_in_progress_marks_all_completed_as_suspicious_and_raises_the_error():
    """
    """

def test_errors_are_raised_with_dask_and():
    """
    Ensure that during a map where there are errors, the other iterations are not affected and still return
    :return:
    """


def test_execution_begins_when_exiting_context_not_during():
    """
    Ensures that the user can "queue" dependent actions
    :return:
    """

def test_deletion_removes_item_from_queue_if_in_progress():
    pass


def test_map_sets_incomplete_attr_True_in_simfile_before_execution_and_after_prep():
    pass

def test_map_sets_incomplete_attr_False_in_simfile_after_successful_execution():
    pass

def test_map_sets_incomplete_attr_True_in_index_before_execution_and_after_prep():
    pass

def test_map_sets_incomplete_attr_False_in_index_after_successful_execution():
    pass


def test_map_still_uses_individual_files_even_after_aggregation_is_declared_in_the_same_execution_context():
    """
    Ensure that in cases such as:
    with spec:
        spec.map(function, ['result'], ...)
        spec.aggregate('result')
        spec.map(new_function_that_uses_result, ...)
    the second map still uses the future results from the first map
    :return:
    """