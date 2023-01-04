station_name = 'csv_piping'
consumer_group_name = 'csv_consumer_group'
connection_dict = {
    'host': 'localhost',
    'username': 'ddy',
    'connection_token': 'memphis'
}
consumer_batch_size=10
idempotency_window_ms= 5 # no need for a long idempotency_window_ms as each producer task has its own unique task_id
retention_value=3600 # 30 minutes