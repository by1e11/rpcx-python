from calendar import c
import json
import io
import re
from urllib import request, response

import msgpack

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

CONST_MAGIC_NUMBER = 0x08
CONST_VERSION = 0x01


class Header:
    def __init__(self):
        self.magic_number: int = None
        self.version: int = CONST_VERSION
        self.message_type: int = MessageType.Request
        self.heartbeat = False
        self.oneway = False
        self.compress_type: int = CompressType.DoNotCompress  # At present support only Normal！
        self.message_status_type: int = MessageStatusType.Normal
        self.serialize_type: int = SerializeType.Json  # At present support only json！
        self.reserved = 0

    def to_bytes(self):
        result = self.magic_number.to_bytes(1) + self.version.to_bytes(1)
        result += bytes((self.message_type << 7 | self.heartbeat << 6 | self.oneway << 5
                         | self.compress_type << 2 | self.message_status_type,))
        result += bytes((self.serialize_type << 4 | self.reserved,))
        return result

    def decode(self, header: bytes):
        assert len(header), 'header decode error, len must be 4'
        self.magic_number = header[0]
        if not self.check_magic_number():
            raise ValueError('magic number error')
        self.version = header[1]
        byte3 = header[2]
        byte4 = header[3]
        self.message_type = byte3 >> 7
        self.heartbeat = 0b00000001 & (byte3 >> 6)
        self.oneway = 0b00000001 & (byte3 >> 5)
        self.compress_type = 0b00000111 & (byte3 >> 2)
        self.message_status_type = 0b00000011 & byte3
        self.serialize_type = 0b00001111 & (byte4 >> 4)
        self.reserved = 0b00001111 & byte4

    def check_magic_number(self):
        return self.magic_number == CONST_MAGIC_NUMBER

class Message:
    def __init__(self, service_path=None, service_method=None, payload=None, metadata=None, message_id=None):
        self.header = Header()
        self.message_id: int = message_id or 0
        self.total_size = 0
        self.service_path = service_path
        self.service_method = service_method
        self.metadata = metadata
        self.payload = payload

        self.raw_data: bytes = None

    @property
    def isoneway(self):
        return self.header.oneway

    @property
    def isrequest(self):
        return self.header.message_type == MessageType.Request

    @property
    def isresponse(self):
        return self.header.message_type == MessageType.Response
    
    @property
    def success(self):
        return not self.header.message_status_type

    @property
    def error(self):
        return self.metadata.get('rpcx_error')

    def encode(self):
        assert self.service_path, 'service_path required'
        assert self.service_method, 'service_method required'
        self.header.magic_number = CONST_MAGIC_NUMBER
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
        print(' '.join([hex(int(byte)) for byte in self.encode()]))

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
        data = bytes()
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

    # @classmethod
    # def compose_response(cls, data: bytes):
    #     response = cls()
    #     response.__decode(data)
    #     assert response.header.message_type == MessageType.Response, 'wrong message type'

    #     return response

    def decode(self, data: bytes):
        """
        Decode message from bytes. This handles incomplete messages.
        If data is an incomplete message, it returns False. 
        Only when the message is complete, it decodes the message
        and returns True if successful.
        """
        buf = io.BytesIO(data)

        if self.raw_data is None:
            # decode header
            header = buf.read(4)
            try:
                self.header.decode(header)
            except ValueError as e:
                raise ValueError('decode header error: {}'.format(e))

            buf.read(8) # skip unused message_id
            self.total_size = int.from_bytes(buf.read(4), 'big')
            self.raw_data = bytes()

        # import pdb; pdb.set_trace()

        self.raw_data += data
        if self.total_size > len(self.raw_data):
            return False
        
        self.__decode()

        self.raw_data = None

        return True
        
    def __decode(self):
        buf = io.BytesIO(self.raw_data)
        buf.read(4) # skip header
        self.message_id = int.from_bytes(buf.read(8), 'big')
        buf.read(4) # skip total size
        service_path_size = int.from_bytes(buf.read(4), 'big')
        service_path = buf.read(service_path_size)
        service_method_size = int.from_bytes(buf.read(4), 'big')
        service_method = buf.read(service_method_size)
        metadata_size = int.from_bytes(buf.read(4), 'big')
        metadata = buf.read(metadata_size)
        playload_size = int.from_bytes(buf.read(4), 'big')
        playload = buf.read(playload_size)

        # check data
        assert self.total_size == sum([
            service_path_size,
            service_method_size,
            metadata_size,
            playload_size,
            16
        ]), 'parse data error'

        self.__decode_service_path(service_path)
        self.__decode_service_method(service_method)
        self.__decode_metadata(metadata)
        self.__decode_playload(playload)

    @property
    def complete(self):
        return self.total_size == len(self.data)

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

    def __decode_playload(self, playload: bytes):
        if not playload:
            return
        if self.header.serialize_type == SerializeType.Json:
            data = playload.decode('utf-8')
            self.payload = json.loads(data)
        elif self.header.serialize_type == SerializeType.MessagePack:
            self.payload = msgpack.loads(playload)
        else:
            assert False, 'At present support only json！'
