"""
Can keep pipelines in memory without constant read/writes for map, reduce, filter
but when we get to an aggregate/execute we need to read everything at once and from disk: we can't keep everything in memory at that point
do a `.partition(write_batch_size).map(write_to_aggregation_file_return_ids).partition(all_data_len).map(group-ids)`
for aggregation since we can write in batches

"""