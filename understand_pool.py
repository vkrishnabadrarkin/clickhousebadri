from aioch import Client
import asyncio
async def showoff(loop):
    
pool = {}
for i in range(10):
    client = Client("localhost")
    pool[i] = {"c": client, "a": True}
for i, val in pool.items():
    print(i,val)
loop = asyncio.get_event_loop()
loop.run_until_complete(showoff(loop))