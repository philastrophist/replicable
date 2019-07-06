"""
Test that parameter specifications are written to index
Test that subsets are written to index
Test that disjoint parameter sets e.g.:
    spec1 = Constant('model', model1) + Constant('a', [1,2,3]) + Constant('b', [100, 2])
    spec2 = Constant('model', model2) + Constant('c', [10,20,30])
    spec = spec1 + spec2

    with spec:
        spec.map(build_data, ['data'])  # would build common data for both models
        spec[spec['model'] == model1].map(build_data1, ['data1'])
        spec[spec['model'] == model1].map(build_data2, ['data2'])
        spec.unify(['data1', 'data2'], 'data') # joins them up (but does not persist -unnecessary) if they are congruent
write nans to non-overlapping parameters and sets subset
write unification

spec.read('a', 'b')  # returns dask delayed object for reading and joining
spec[filt].read('a', 'b')   # does streamz pipeline first to build subset then passes selected filenames to read
"""