import asyncio
import json
import csv
from os import rename, listdir
from os.path import exists
from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError
from src.utils.config import connection_dict, station_name,consumer_group_name, consumer_batch_size, debug
from src.utils.string_utils import generate_hash

consumer_name_prefix = "read_csv_consumer_"
output_file_prefix = "output_"
failed_file_suffix = ".failed"
max_file_lookups = 3 # Max number of times to look for a file in a single iteration
max_retries = 30 # Max number of retries for a single file+task

def move_file_to_failed_state(filepath, data, headers):
    print("ERROR: Max retries reached for this file, dropping message & task.", data, headers)
    failed_path = filepath + failed_file_suffix
    existing_partial = [path for path in listdir('.') if path.startswith(filepath)]
    if len(existing_partial) > 0:
        print("DEBUG: Partial file(s) exist for this task, moving to failed state: ", existing_partial) if debug else None
        for partial_file in existing_partial:
            rename(partial_file, failed_path)
    else:
        print("DEBUG: No partial file(s) exist for this task, creating empty failed file as a flag: ", filepath) if debug else None
        f = open(failed_path, "w")
        f.close()

async def main():
    async def msg_handler(msgs, error):
        retries = {}
        if not msgs:
            return
        queue = [*msgs]
        msg = queue.pop(0)
        while(msg):
            try:
                data = json.loads(msg.get_data())
                headers = msg.get_headers()
                if error or not data or not headers:
                    print("ERROR: ", error)
                    continue
                filename = headers.get("filename")
                task_id = headers.get("task_id")
                filepath = output_file_prefix + task_id + '.' + filename
                line_number = data.get("line_number")
                eof = data.get("eof")
                print("DEBUG: Got line number: ", line_number, " for file: ", filename, " with task_id: ",task_id) if debug else None
                filepath_ready_for_write = filepath + "." + str(line_number)

                open_mode = "w" if line_number == 0 else "a"
                if exists(filepath):
                        print("ERROR: File has already finished transferring for this task, dropping message.", data, headers)
                        continue
                elif exists(filepath+failed_file_suffix):
                        print("DEBUG: File has already failed for this task, dropping message.", data, headers) if debug else None
                        continue
                if line_number == 0:
                    if exists(filepath_ready_for_write):
                        print("DEBUG: Initial file already exists for this task, Overriding.") if debug else None
                    else:
                        print("Starting a new file: ", filename, " for task_id: ", task_id)
                else: 
                    lookup = 0
                    while not exists(filepath_ready_for_write):
                        if lookup % 10 == 0:
                            print("DEBUG: Waiting for file '", filepath, "' to be ready for line number: ", line_number, " via task_id: ", task_id, ".") if debug else None
                        lookup += 1
                        if lookup > max_file_lookups:
                            break
                        await asyncio.sleep(0.1)
                    if lookup > max_file_lookups:
                        retry_number = retries.get(filepath_ready_for_write, 0)
                        if retry_number < max_retries:
                            retries[filepath_ready_for_write] = retry_number + 1
                            print("DEBUG: File is not ready, re-queueing message locally: ", data, headers) if debug else None
                            queue.append(msg)
                        else:
                            move_file_to_failed_state(filepath, data, headers)
                        continue

                with open(filepath_ready_for_write, open_mode) as csv_file:
                    # By default, csv.writer() inserts a new line after EVERY row. we want to avoid that for the last row.
                    csv.writer(csv_file, lineterminator=("" if eof else "\r\n")).writerow(data.get("row"))
                    # csv_file.write(",".join(data.get("row")) + ("" if eof else "\r\n"))
                
                filepath_next_path = filepath if eof else filepath + "." + str(line_number + 1)
                rename(filepath_ready_for_write, filepath_next_path)
                if eof:
                    print("~~FINISHED~~: Last event (EOF) has been received for the file: ", filename, " via task_id: ", task_id)
                await msg.ack()

                
            except (MemphisError, MemphisConnectError, MemphisHeaderError) as e:
                print("ERROR: ", e)
                return
            finally:
                msg = queue.pop(0) if queue else None
            
    try:
        memphis = Memphis()
        await memphis.connect(**connection_dict)
        consumer_id = generate_hash()
        consumer = await memphis.consumer(station_name=station_name, consumer_name=consumer_name_prefix+consumer_id, consumer_group=consumer_group_name, max_msg_deliveries=1, batch_size=consumer_batch_size)

        consumer.consume(msg_handler)
        while memphis.is_connection_active:
            await asyncio.sleep(0.1)
        
    except (MemphisError, MemphisConnectError) as e:
        print("ERROR: ", e)
        
    finally:
        await consumer.destroy()
        await memphis.close()
        
if __name__ == '__main__':
    asyncio.run(main())
        