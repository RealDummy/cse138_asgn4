import asyncio
from threading import Thread

#each thread needs one Executor for sending broadcast messages in the background
class Executor:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        thread = Thread(target=__class__._run, args=[self.loop], daemon=True)
        thread.start()

    def _run(loop: asyncio.AbstractEventLoop):
        asyncio.set_event_loop(loop)
        loop.run_forever()
    
    def run(self, asyncFuncRet):
        return asyncio.run_coroutine_threadsafe(asyncFuncRet, self.loop)