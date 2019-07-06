
"""
Scenarios:

Parameters (some/all/None) -> reduce
Result (1, 2) -> reduce
Combinations of above -> reduce
Filter[parameter],Filter[result],Filter[parameter+result],None
= (3 + 2 + 6) * 4 = 44 tests
test: data recovered (shape,dtype, values), description recovered, tasks marked as complete, locations in index, subsets stored in index
test: running again does not do any work
"""