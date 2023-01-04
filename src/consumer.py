import asyncio
import json
from os import rename
from os.path import exists
from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError
from src.utils.config import connection_dict, station_name,consumer_group_name
from src.utils.string_utils import generate_hash

consumer_name_prefix = "read_csv_consumer_"
output_file_prefix = "output_"
max_file_lookups = 20 # Max number of times to look for a file in a single iteration
max_retries = 5 # Max number of retries for a single file+task

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
                print("DEBUG: Got line number: ", line_number, filename, task_id)
                filepath_ready_for_write = filepath + "." + str(line_number)

                open_mode = "w" if line_number == 0 else "a"
                if exists(filepath):
                        print("ERROR: File has already finished transferring for this task, dropping message.", data, headers)
                        continue
                if line_number == 0:
                    if exists(filepath_ready_for_write):
                        print("Initial file already exists for this task, Overriding.")
                else: 
                    lookup = 0
                    while not exists(filepath_ready_for_write):
                        if lookup % 10 == 0:
                            print("Waiting for file to be ready")
                        lookup += 1
                        if lookup > max_file_lookups:
                            break
                        await asyncio.sleep(0.1)
                    if lookup > max_file_lookups:
                        retry_number = retries.get(filepath_ready_for_write, 0)
                        if retry_number < max_retries:
                            retries[filepath_ready_for_write] = retry_number + 1
                            print("File is not ready, re-queueing message locally: ", data, headers)
                            queue.append(msg)
                        else:
                            print("ERROR: Max retries reached for this file, dropping message.", data, headers)
                        continue

                with open(filepath_ready_for_write, open_mode) as csv_file:
                    csv_file.write(",".join(data.get("row")) + ("" if eof else "\n"))
                
                filepath_next_path = filepath if eof else filepath + "." + str(line_number + 1)
                rename(filepath_ready_for_write, filepath_next_path)
                if eof:
                    print("~~FINISHED~~: Last event (EOF) has been received for the file: ", filename, " via task_id: ", task_id)
                await msg.ack()

                
            except (MemphisError, MemphisConnectError, MemphisHeaderError) as e:
                print(e)
                return
            finally:
                msg = queue.pop(0) if queue else None
            
    try:
        memphis = Memphis()
        await memphis.connect(**connection_dict)
        consumer_id = generate_hash()
        consumer = await memphis.consumer(station_name=station_name, consumer_name=consumer_name_prefix+consumer_id, consumer_group=consumer_group_name, max_msg_deliveries=1, batch_size=5)

        consumer.consume(msg_handler)
        while memphis.is_connection_active:
            await asyncio.sleep(1)
        
    except (MemphisError, MemphisConnectError) as e:
        print("ERROR: ", e)
        
    finally:
        await consumer.destroy()
        await memphis.close()
        
if __name__ == '__main__':
    asyncio.run(main())
        