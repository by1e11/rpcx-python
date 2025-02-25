import base64
import io
from PIL import Image
from rpcx.server import RPCServer, RPCService, RPCManager
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

class ImageService(RPCService):
    async def base64_image(self, base64_string: str):
       decoded_string = io.BytesIO(base64.b64decode(base64_string))
       img = Image.open(decoded_string)
       img.show()

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
