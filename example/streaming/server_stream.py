import time
from rpcx.server import RPCServer, RPCService, RPCManager, Stream
from anyio import run

class TestServiceStream(RPCService):
    async def fibbonacci(self, N: int, stream: Stream):
        a, b = 0, 1

        for i in range(N):
            await stream.send({"C": a})
            a, b = b, a + b
            # simulate slow processing
            time.sleep(1)

async def main():
    manager = RPCManager()
    server = RPCServer(manager)
    await server.serve()

if __name__ == '__main__':
    try:
        run(main)
    except KeyboardInterrupt:
        print("\nCtrl-C pressed. Bye!")
        pass
