
"""
Scenarios:

Parameters (some/all/None) -> map
Result (1, 2) -> map
Combinations of above -> map
Filter[parameter],Filter[result],Filter[parameter+result],None
= (3 + 2 + 6) * 4 = 44 tests
test: data recovered (shape,dtype, values), description recovered, tasks marked as complete, locations in index, subsets stored in index
test: running again does not do any work
"""
import pytest


@pytest.parametrize.fixture
def specification():
    pass


