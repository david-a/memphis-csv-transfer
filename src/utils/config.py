debug=True
station_name = 'csv_piping'
consumer_group_name = 'csv_consumer_group'
connection_dict = {
    'host': 'localhost',
    'username': 'ddy',
    'connection_token': 'memphis'
}
consumer_batch_size=10
idempotency_window_ms= 5000 # no need for a long idempotency_window_ms as each producer task has its own unique task_id
retention_value=3600 # 30 minutes

# Bloom filter
use_bloom_filter=False
typical_row_size_in_chars = 100
typical_row_size_in_bytes = len(('é' * typical_row_size_in_chars).encode('utf-8'))
bloom_max_items_count = 10000

