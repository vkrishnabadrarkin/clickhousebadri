import asyncio
import inserter_config
from aioch import Client
import time
import datetime
num = 0
def createlog(took):
    with open("Badlog.txt",'a') as f:
        text = f"{num}{time.ctime(time.time())} - " \
                f"table : {inserter_config.TABLE_NAME} - " \
                f"time_taken : {round(took,2)} - " \
                f"db : {inserter_config.DB_NAME} - \n"
        f.write(text)
        num = num+1
pool = {}
def create_pool():
    for i in range(inserter_config.WORKERS):
        client = Client(inserter_config.HOST)
        pool[i] = {"c": client, "a": True}
async def fill_events(loop,EventsPerDay,BulkSize):
        
        print('hi')
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    create_pool()
    start = time.time()
    print("DATA insertion started at ", time.ctime(time.time()))
    loop.run_until_complete(fill_events(loop, inserter_config.EVENTS_PER_DAY, inserter_config.BULK_SIZE))
    end = time.time()
    took = end-start
    print(f"insertion completed at {time.ctime(end)} and took  {took }seconds")
    createlog(took)