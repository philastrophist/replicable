"""
execution requires all of the specified data at once so we can't trust that dask won't crash, lets necessitate that
assembly is required before execution `assembly...(returns ids).map(group-ids).map(execution)
"""