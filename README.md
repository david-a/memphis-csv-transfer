## Message Broker Example: CSV piping

This is a message broker example program with-

1. A producer which reads a csv file line by line and emits them as JSON events into the broker
2. A scalable consumer which receives the lines from the broker and reassemble the original csv file

## How to use

Setup (First time only) -

1. Clone the repository and change to the root directory of it
2. Create a virtual env with `python3 -m venv .pyenv`
3. Activate the env with `source .pyenv/bin/activate`
4. Install requirements with `pip3 install -r requirements.txt`

Run -

1. If not already done, cd to the repo root and activate the env with `source .pyenv/bin/activate`
2. Run the docker env with `docker compose -f docker-compose.yml -p memphis up`
3. Create a station with `python3 -m src.station create` # In order to avoid using the default settings when running a consumer/producer
4. Run each consumer with `python3 -m src.consumer`
   (You can set `debug=False` on `utils/config.py` to avoid debug messages on screen)
5. Run the producer with `python3 -m src.producer example.csv` # or some other csv filepath
6. Destroy a station with `python3 -m src.station destroy`

## External libs

1. `asyncio` and `memphis-py` - Required for Memphis broker
2. For the Bloom filter, I've added `pymmh3` to have a fast hashing algorithm with manual seed input (as opposed to using built in `hash()` which does not support manual seed) so I can have multiple hashes per each entry, and `bitarray` to efficiently use bit sized array with low memory footprint (as opposed to using the built in bytearray, or having to implement a bitarray myself using it).

## Notes

1. The app can handle multiple files simultaniously and dynamically (i.e. no need for separate consumer group for each task/file), separates them by their filename.
2. The write-lock mechanism and ordering mechanism is one - each row is written to the file only when it is ready for it - the filename ends with the relevant line number (e.g. output_12345.example.csv.13). This mechanism is better optimized to large files, the bigger the consumer batch size is.
3. I chose to drop the task (all the next mesages) completely if a message (i.e. row) is missing to assure file integrity, but I could just as well replace the row with some placeholder.
4. I set idempotency_window_ms to a small value since each run of the producer on a file will create a separate task, causing the consumers to create a separate file for it. So using idempotency_window_ms only to avoid networking issues duplications and such. Nontheless if we want to avoid sending the same files again we only need to set idempotency_window_ms to the desired period.
5. I assume that the consumer does not need to know the context of each row (e.g. its column names), and as such I can send the rows without this unnecessary data. Each line will be written to the right file and order, nothing more, as I need to open and close the file for each line anyhow in order to support multiple consumers and as required in the spec.
6. If that context (column names) would be needed on a per row basis, I could do ONE of the following -
   a. In the producer, send the titles inside the json data or headers (waste of memory, network, etc)
   b. In the consumer, read the titles from the first line of file being written (or have them from the first message of the file) -
   b.1. EACH time I get a message (enabling multiple files, but affecting performance and file io)
   b.2. On the first time I handle a file (designing the app to support only a single file at a time, better performance)
   b.3. On the first time I handle a new file to the consumer, caching the titles per file (enjoying both worlds)
7. I've implemented a **Bloom filter** and used it on the producer. Under the assumptions listed above, it is not needed so much, as we have idempotency_window_ms and filename + row number based dedup mechanism (dedup by SAME line AND SAME content), but I've implemented it for demonstrations purposes. It should be enabled with setting `use_bloom_filter=True` in `utils/config.py` **ONLY if a you need a statistic filtering of rows** by hashing their content.
8. Creds are hard-coded for simplicity and minimum dependencies but of course these would be handled as env vars in a prod scenario, maybe using dotenv file for development/build envs.
9. This file transfer mechanism can easily be converted to be type agnostic (and a bit faster I guess) if using simple file read/write instead of csv lib's ones. The rest of the considerations are already taken into account.

## Issues

1. In memphis.py (inside the pip package), I've disabled lines 302-303 in order to avoid errors when running the producer
   a. `self.station_schemaverse_to_dls[station_name_internal] = create_res['schemaverse_to_dls']` # Error was `memphis: 'schemaverse_to_dls'`
   b. `self.cluster_configurations['send_notification'] = create_res['send_notification']` # Error was `memphis: 'send_notification'`
   I don't know if it's something with my environment or a bug in the python sdk (when used the JS one it didn't happen) but it replicated both using the local broker and the sandbox.
2. `dedup_enabled` shows up as a `memphis.station()` option in the docs but not yet supported, at least in the python sdk.
