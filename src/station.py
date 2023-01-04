import sys
import asyncio
from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError,retention_types, storage_types
from src.utils.config import connection_dict, station_name, idempotency_window_ms, retention_value

def read_action_from_args():
    if len(sys.argv) < 2 or sys.argv[1] not in ["create", "destroy"]:
        print("Usage: python3 -m src.station create/destroy")
        sys.exit(1)
    return sys.argv[1]

async def main():        
    try:
        memphis = Memphis()
        await memphis.connect(**connection_dict)
        
        action = read_action_from_args()
        station = await memphis.station(name=station_name, idempotency_window_ms=idempotency_window_ms,retention_value=retention_value) 
        if action == "destroy":
            await station.destroy()
        
    except (MemphisError, MemphisConnectError) as e:
        print(e)
        
    finally:
        await memphis.close()
        
if __name__ == '__main__':
    asyncio.run(main())
        