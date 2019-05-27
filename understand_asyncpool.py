import asyncpool
import logging
from aioch import Client
#import inspect
import asyncio
change_var = 1
#import asyncio
#print(dir(asyncpool))
#print(dir(asyncio))
#print(dir(asyncpool.asyncio))
#print(dir(asyncpool.AsyncPool))
#async with asyncpool.AsyncPool()
#print(inspect.getfullargspec(asyncpool.AsyncPool))
#pool = {}
#for i in range(10):
client = Client("localhost")
#    pool[i] = {"c": client, "a": True}
async def do_something(king):
    global change_var
    #print (f"{change_var} you added a new thing hsd {king}")
    await client.execute('SHOW DATABASES')
    change_var = change_var+1
async def test_async(loop):
    async with asyncpool.AsyncPool(
        loop, 
        num_workers = 10, 
        worker_co = do_something,
        max_task_time=300,
        log_every_n=10,
        name = "CHPOOLER", 
        logger = logging.getLogger("CHPOOLER")) as P:
        for i in range(4):
            await P.push(i+200)
loop = asyncio.get_event_loop()
loop.run_until_complete(test_async(loop))