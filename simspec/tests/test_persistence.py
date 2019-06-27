import pytest


def test_directory_structure():
    """
    Make sure output directory is:
        project/
            {environment_hash}-{source_hash}/
                {param1-hash}.h5
    """



def test_index_on_result_name_returns_delayed_read_in_different_context_to_the_execution_context():
    pass


def test_index_on_param_name_returns_delayed_read_in_different_context_to_the_execution_context():
    pass


def test_index_on_result_name_returns_delayed_which_waits_for_files_in_same_context_as_the_execution_context():
    pass


def test_map_store_and_read_back():
    """
    Ensure written data and metadata is correct
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

def test_deletion_removes_results_from_each_file_leaving_everything_else():
    pass


def test_deletion_removes_result_aggregation_file_leaving_everything_else():
    pass


def test_disallow_duplicate_names():
    """
    Ensure that duplicate names are detected before processing
    :return:
    """

def test_single_errored_map_doesnt_impact_others():
    pass


def test_unstructured_data_cannot_be_read_over_the_index():
    """
    If you store data without specify that the shape and dtype will be consistent, then you can't expect to index along
    the index dimension. i.e.

    with spec:
        spec.map(function, ['result'], structure=[(float, int)])

        spec[

    :return:
    """

def test_execution_begins_when_exiting_context_not_during():
    """
    Ensure that the user can "queue" dependent actions
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


@pytest.mark.parametrize('dtype', [str, float, int, bool])
@pytest.mark.parametrize('shape', [(1,), (2, 3)])
def test_single_errored_map_is_retrieved_as_nan(dtype, shape):
    """
    Ensure that dask returns nan with the correct shape for missing data
    :param dtype:
    :param shape:
    :return:
    """

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