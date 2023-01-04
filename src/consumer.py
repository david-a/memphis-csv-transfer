import asyncio
import json
from os import rename
from os.path import exists
from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError
from src.utils.config import connection_dict, station_name,consumer_group_name
from src.utils.string_utils import generate_hash

consumer_name_prefix = "read_csv_consumer_"
output_file_prefix = "output_"
max_retries = 100

async def main():
    async def msg_handler(msgs, error):
        try:
            for msg in msgs:
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
                if line_number == 0:
                    if exists(filepath) or exists(filepath_ready_for_write):
                        print("ERROR: File already exists for this task, dropping message.", data, headers)
                        continue
                else: 
                    retry = 0
                    while not exists(filepath_ready_for_write):
                        if retry % 10 == 0:
                            print("Waiting for file to be ready")
                        retry += 1
                        if retry > max_retries:
                            print("ERROR: File was not ready, dropping message.", data, headers)
                            break
                        await asyncio.sleep(0.1)
                    if retry > max_retries:
                        continue
                with open(filepath_ready_for_write, open_mode) as csv_file:
                    csv_file.write(",".join(data.get("row")) + ("" if eof else "\n"))
                
                filepath_next_path = filepath if eof else filepath + "." + str(line_number + 1)
                rename(filepath_ready_for_write, filepath_next_path)
                if eof:
                    print("EOF: Last event has been received for the file: ", filename, " via task_id: ", task_id)
                await msg.ack()

                
        except (MemphisError, MemphisConnectError, MemphisHeaderError) as e:
            print(e)
            return
        
    try:
        memphis = Memphis()
        await memphis.connect(**connection_dict)
        consumer_id = generate_hash()
        consumer = await memphis.consumer(station_name=station_name, consumer_name=consumer_name_prefix+consumer_id, consumer_group=consumer_group_name, max_msg_deliveries=1, batch_size=1)

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
        