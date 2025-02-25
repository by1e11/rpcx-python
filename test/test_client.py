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

class MessageType:
    Request = 0
    Response = 1


class CompressType:
    DoNotCompress = 0
    GZIP = 1


class MessageStatusType:
    Normal = 0
    Error = 1


class SerializeType:
    Raw = 0
    Json = 1
    Protobuf = 2
    MessagePack = 3


class Header:
    def __init__(self):
        self.magic_number = bytes((0x08,))
        self.version = bytes((0,))
        self.message_type: int = MessageType.Request
        self.heartbeat = False
        self.oneway = False
        self.compress_type: int = CompressType.DoNotCompress  # At present support only Normal！
        self.message_status_type: int = MessageStatusType.Normal
        self.serialize_type: int = SerializeType.MessagePack # At present support only json！
        self.reserved = 0

    def to_bytes(self):
        result = self.magic_number + self.version
        result += bytes((self.message_type << 7 | self.heartbeat << 6 | self.oneway << 5
                         | self.compress_type << 2 | self.message_status_type,))
        result += bytes((self.serialize_type << 4 | self.reserved,))
        return result

    def decode(self, header: bytes):
        assert len(header), 'header decode error, len must be 4'
        bit3 = header[2]
        bit4 = header[3]
        self.message_type = bit3 >> 7
        self.heartbeat = 0b00000001 & (bit3 >> 6)
        self.oneway = 0b00000001 & (bit3 >> 5)
        self.compress_type = 0b00000111 & (bit3 >> 2)
        self.message_status_type = 0b00000011 & bit3
        self.serialize_type = 0b00001111 & (bit4 >> 4)
        self.reserved = 0b00001111 & bit4


class Message:
    def __init__(self, service_path=None, service_method=None, payload=None, metadata=None, message_id=None):
        self.header = Header()
        self.message_id: int = message_id or 0
        self.service_path = service_path
        self.service_method = service_method
        self.metadata = metadata
        self.payload = payload


class Request(Message):
    def __init__(self, heartbeat=False, oneway=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.header.heartbeat = heartbeat
        self.header.oneway = oneway

    def to_bytes(self):
        assert self.service_path, 'service_path required'
        assert self.service_method, 'service_method required'
        data = [
            self.__encode_service_path(),
            self.__encode_service_method(),
            self.__encode_metadata(),
            self.__encode_payload()
        ]
        total_size = sum(map(lambda d: int.from_bytes(d[0], 'big'), data)) + 16
        result = b''.join([
            self.header.to_bytes(),
            self.__encode_message_id(),
            total_size.to_bytes(4, 'big'),
            b''.join(map(lambda d: b''.join(d), data))
        ])
        return result

    def dump(self):
        print(' '.join([hex(int(byte)) for byte in self.to_bytes()]))

    def __encode_message_id(self):
        data = self.message_id.to_bytes(8, 'big')
        return data

    def __encode_metadata(self):
        result = bytes()
        if not self.metadata:
            return bytes(4), bytes()
        for key, value in self.metadata.items():
            key = key.encode('utf-8')
            value = value.encode('utf-8')
            result += len(key).to_bytes(4, 'big')
            result += key
            result += len(value).to_bytes(4, 'big')
            result += value
        size = len(result).to_bytes(4, 'big')
        return size, result

    def __encode_service_path(self):
        data = self.service_path.encode('utf-8')
        size = len(data).to_bytes(4, 'big')
        return size, data

    def __encode_service_method(self):
        data = self.service_method.encode('utf-8')
        size = len(data).to_bytes(4, 'big')
        return size, data 

    def __encode_payload(self):
        data: bytes = bytes()
        if not self.payload:
            return bytes(4), bytes()
        if self.header.serialize_type == SerializeType.Json:
            data = json.dumps(self.payload).encode('utf-8')
        elif self.header.serialize_type == SerializeType.MessagePack:
            data = msgpack.dumps(self.payload)
        else:
            assert False, 'At present support only json！'
        size = len(data).to_bytes(4, 'big')
        return size, data

class Response(Message):
    def __init__(self, data: bytes):
        super().__init__(metadata={})
        self.__decode(data)

    def __decode(self, data: bytes):
        buf = io.BytesIO(data)
        header = buf.read(4)
        message_id = buf.read(8)
        total_size = int.from_bytes(buf.read(4), 'big')
        service_path_size = int.from_bytes(buf.read(4), 'big')
        service_path = buf.read(service_path_size)
        service_method_size = int.from_bytes(buf.read(4), 'big')
        service_method = buf.read(service_method_size)
        metadata_size = int.from_bytes(buf.read(4), 'big')
        metadata = buf.read(metadata_size)
        playload_size = int.from_bytes(buf.read(4), 'big')
        playload = buf.read(playload_size)

        # check data
        assert total_size == sum([
            service_path_size,
            service_method_size,
            metadata_size,
            playload_size,
            16
        ]), 'parse data error'

        self.header.decode(header)
        self.__decode_message_id(message_id)
        self.__decode_service_path(service_path)
        self.__decode_service_method(service_method)
        self.__decode_metadata(metadata)
        self.__decode_playload(playload)

    def __decode_message_id(self, message_id):
        self.message_id = int.from_bytes(message_id, 'big')

    def __decode_service_path(self, service_path):
        self.service_path = service_path.decode('utf-8')

    def __decode_service_method(self, service_method):
        self.service_method = service_method.decode('utf-8')

    def __decode_metadata(self, metadata):
        if not metadata:
            return
        buf = io.BytesIO(metadata)
        while buf.tell() < len(metadata):
            key_size = int.from_bytes(buf.read(4), 'big')
            key = buf.read(key_size).decode('utf-8')
            value_size = int.from_bytes(buf.read(4), 'big')
            value = buf.read(value_size).decode('utf-8')
            self.metadata[key] = value

    def __decode_playload(self, playload):
        if not playload:
            return
        data = playload.decode('utf-8')
        if self.header.serialize_type == SerializeType.Json:
            self.payload = json.loads(data)
        else:
            assert False, 'At present support only json！'

    @property
    def success(self):
        return not self.header.message_status_type

    @property
    def error(self):
        return self.metadata.get('rpcx_error')

class Client:
    def __init__(self, host, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port

    def call(self, service_path, method_name, args=None, meta=None, msg_id=None, heartbeat=False, oneway=False):
        request = Request(service_path=service_path, service_method=method_name, payload=args, metadata=meta,
                          message_id=msg_id, heartbeat=heartbeat, oneway=oneway)
        self.socket.connect((self.host, self.port))
        self.socket.send(request.to_bytes())
        if oneway:
            self.socket.close()
            return

        result = [b''] * 2
        result[0] = self.socket.recv(16)
        body_len = int.from_bytes(result[0][-4:], 'big') + 16
        result[1] = self.socket.recv(body_len)
        self.socket.close()
        return Response(b''.join(result))


if __name__ == '__main__':
    client = Client('localhost', 33632)
    # response = client.call('TestService', 'sub', dict(a=2, b=3))
    # response = client.call('TestService', 'large_data', dict(text='a' * 100000))
    from res.test_image import test_image
    response = client.call('ImageService', 'base64_image', dict(base64_string=test_image))
    if response.success:
        print(len(response.payload) if response.payload else 0)
    else:
        print(response.error)