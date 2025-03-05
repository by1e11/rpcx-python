# -*- coding:utf-8 -*-
"""
the rpcx client for python
author: jiy
mail: hyhkjiy@163.com
"""

import json
import msgpack
import io

import socket

from rpcx.message import Message

class Client:
    def __init__(self, host, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port

    def receive(self):
        result = [b''] * 2
        result[0] = self.socket.recv(16)
        body_len = int.from_bytes(result[0][-4:], 'big') + 16
        result[1] = self.socket.recv(body_len)
        result = b''.join(result)
        # import pdb; pdb.set_trace()
        response = Message()
        response.decode(result)
        return response

    def call(self, service_path, method_name, args=None, meta=None, msg_id=None, heartbeat=False, oneway=False)->Message:
        request = Message(service_path=service_path, service_method=method_name, payload=args, metadata=meta, message_id=msg_id)
        self.socket.connect((self.host, self.port))
        self.socket.send(request.encode())
        if oneway:
            self.socket.close()
            return
        
        chunk = self.receive()
        while chunk.metadata and 'is_chunk' in chunk.metadata:
            print(f'receive chunk {chunk.payload}')
            chunk = self.receive()
            
        self.socket.close()
        # return last chunk (steam_end)
        return chunk


if __name__ == '__main__':
    client = Client('172.27.50.55', 24800)
    # response = client.call('TestService', 'sub', dict(a=2, b=3))
    # response = client.call('TestService', 'large_data', dict(text='a' * 100000))
    # from res.test_image import test_image
    # response = client.call('ImageService', 'base64_image', dict(base64_string=test_image))
    # response = client.call('TestServiceStream', 'fibbonacci', dict(n=6))
    response = client.call('ServiceEntry', 'create_session', dict(user_id="developer"))
    if response.success:
        print(len(response.payload) if response.payload else 0)
    else:
        print(response.error)
