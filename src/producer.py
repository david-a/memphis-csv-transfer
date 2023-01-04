import sys
import asyncio
import csv
import json
import hashlib
from os import stat
from memphis import Memphis, Headers, MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError
from src.utils.string_utils import generate_hash
from src.utils.config import connection_dict, station_name

producer_name_prefix = "read_csv_producer_"

def read_csv_file_path():
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.producer <csv-file-path>")
        sys.exit(1)
    return sys.argv[1]

async def produce_row(producer, headers, msg_id_prefix, row, line_number, last=False):
    base = { "row": row, "line_number": line_number }
    serialized = json.dumps({ **base, "eof": True } if last else base)
    await producer.produce(bytearray(serialized, 'utf-8'), headers=headers, msg_id=msg_id_prefix + str(line_number))

async def main():
    try:
        memphis = Memphis()
        await memphis.connect(**connection_dict)
        task_id = generate_hash()
        producer = await memphis.producer(station_name=station_name, producer_name=producer_name_prefix + task_id)

        filepath = read_csv_file_path()
        filename = filepath.split('/')[-1]
        filename_hash = hashlib.md5(filename.encode()).hexdigest()
        print("filename_hash: ", filename_hash)

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
                except StopIteration:
                    if prev_row is None:
                        print("Error: Empty file")
                        break

                    # By default an empty last line of a file is not considered a new line, so we need to check if the last character is a new line
                    size = stat(filename).st_size
                    csv_file.seek(size-1, 0)
                    last_char = csv_file.read()
                    await produce_row(producer, headers, msg_id_prefix, prev_row, line_number, last=last_char != '\n')
                    if last_char == '\n':
                        await produce_row(producer, headers, msg_id_prefix, [], line_number + 1, last=True)
                    break
                else:
                    if prev_row is not None:
                        await produce_row(producer, headers, msg_id_prefix, prev_row, line_number)
                finally:
                    line_number += 1
        
        
    except (MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError) as e:
        print(e)
        
    finally:
        await producer.destroy()
        await memphis.close()
        
if __name__ == '__main__':
    asyncio.run(main())