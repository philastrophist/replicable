"""
Construct steps to perform in spec context: map/reduce/filter
run them one at a time
delete/corrupt files between steps
design function to error during map/reduce/filter for one parameterset

tests: make sure execution continues for other inputs but errors when the dependency is called i.e.:
    with spec:
        spec.map(broken_function, ['name'], ...)  # runs despite errors with parameter=0 (handled and written to log)
        spec.aggregate('name') # stops execution
        spec['name'] # errors
        spec[spec['parameter'] > 0]['name'] # runs

the code above is compiled into streamz with error handling and aggregate only throws an error and stops everything when
running/exit context

"""