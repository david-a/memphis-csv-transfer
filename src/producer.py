import sys
import asyncio
import csv
import json
import hashlib
from os import stat
from memphis import Memphis, Headers, MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError
from src.utils.bloom_filter import BloomFilter
from src.utils.string_utils import generate_hash
from src.utils.config import connection_dict, station_name, debug, bloom_max_items_count, typical_row_size_in_bytes, use_bloom_filter

producer_name_prefix = "read_csv_producer_"

def read_csv_file_path():
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.producer <csv-file-path>")
        sys.exit(1)
    return sys.argv[1]

async def produce_row(filter, producer, headers, msg_id_prefix, row, line_number, last=False):
    if filter and not last and filter.contains(json.dumps(row)):
        print("DEBUG: Row was already produced, Skipping line ", line_number) if debug else None
        return False
    base = { "row": row, "line_number": line_number }
    serialized = json.dumps({ **base, "eof": True } if last else base)
    await producer.produce(bytearray(serialized, 'utf-8'), headers=headers, msg_id=msg_id_prefix + str(line_number))
    if filter:
        filter.add(serialized)
    return True

async def main():
    try:
        memphis = Memphis()
        await memphis.connect(**connection_dict)
        task_id = generate_hash()
        producer = await memphis.producer(station_name=station_name, producer_name=producer_name_prefix + task_id)

        filepath = read_csv_file_path()
        filename = filepath.split('/')[-1]
        filename_hash = hashlib.md5(filename.encode()).hexdigest()
        filesize = stat(filename).st_size
        bloom = BloomFilter(min(bloom_max_items_count, filesize / typical_row_size_in_bytes)) if use_bloom_filter else None # using typical_row_size_in_bytes in order to avoid iterating over the whole file
        print("DEBUG: filename_hash: ", filename_hash) if debug else None

        msg_id_prefix = filename_hash[:6] + "_"
        headers = Headers()
        headers.add("filename", filename)
        headers.add("task_id", task_id)

        with open(filepath, 'r') as csv_file:
            reader = csv.reader(csv_file)
            line_number = -1
            row = None
            
            while True: # Using while + next() instead of for-in to recognise EOF in real time without adding a final empty event
                try:
                    prev_row = row
                    row = next(reader)
                    produced_row = True
                except StopIteration:
                    if prev_row is None:
                        print("ERROR: Empty file")
                        break

                    # By default an empty last line of a file is not considered a new line, so we need to check if the last character is a new line
                    csv_file.seek(filesize-1, 0)
                    last_char = csv_file.read()
                    produced_row = await produce_row(bloom, producer, headers, msg_id_prefix, prev_row, line_number, last=last_char != '\n')
                    if last_char == '\n':
                        produced_row = await produce_row(bloom, producer, headers, msg_id_prefix, [], line_number + 1, last=True)
                    break
                else:
                    if prev_row is not None:
                        produced_row = await produce_row(bloom, producer, headers, msg_id_prefix, prev_row, line_number)
                finally:
                    if produced_row: # skip line_number increment if the row was skipped since this number is used in the consumer lock mechanism and should be continuous
                        line_number += 1
        
        
    except (MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError) as e:
        print(e)
        
    finally:
        await producer.destroy()
        await memphis.close()
        
if __name__ == '__main__':
    asyncio.run(main())