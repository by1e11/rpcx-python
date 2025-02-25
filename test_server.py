from src.rpcx.server import RPCServer, RPCService, RPCManager
from anyio import run

class TestService(RPCService):
    def test_method(self):
        return "test_method_called"

    async def add(self, A: int, B: int):
        return A + B

    async def mul(self, A: int, B: int):
        return {"C": A * B}

    async def large_data(self, text: str):
        return text

async def main():
    manager = RPCManager()
    server = RPCServer(manager)
    await server.serve()

if __name__ == '__main__':
    run(main)
