import socket
import safe_socket

_HEADER_SIZE = 5
_PAYLOAD_SIZE = 65535

_OPCODE_DATA = 2
_OPCODE_ACK = 1
_OPCODE_EOF = 0

_ENCODING = "utf-8"

class Packet:
    def __init__(self):
        self.opcode = None
        self.batches = None
        self.length = None
        self.agency_id = None
        self.raw = None

    def payload(self):
        return self.raw[_HEADER_SIZE : _HEADER_SIZE + self.length]

    def to_bytes(self):
        return self.raw

    def pack_message(self, opcode: int, agency_id: int, payload: str) -> bytes:
        if opcode not in [_OPCODE_DATA, _OPCODE_ACK, _OPCODE_EOF]:
            raise ValueError(f"Opcode must be valid")

        payload_bytes = payload.encode(_ENCODING)
        length = len(payload_bytes)

        if length > _PAYLOAD_SIZE:
            raise ValueError(f"Payload must be lower or equal than {_PAYLOAD_SIZE}")
        
        length_high = (length >> 8) & 0xFF
        length_low = length & 0xFF

        self.opcode = opcode
        self.length = length
        self.agency_id = agency_id
        self.raw = bytes([opcode, 1, length_high, length_low, agency_id]) + payload_bytes

        return self.raw

    def unpack_message(self, client_socket: socket.socket):
        message_header = safe_socket.recv_all(client_socket, _HEADER_SIZE)
        
        if not message_header:
            self.opcode = None
            return

        self.opcode = int(message_header[0])
        self.batches = int(message_header[1])
        self.length = (int(message_header[2]) << 8) | int(message_header[3])
        self.agency_id = int(message_header[4])
        
        message_payload = safe_socket.recv_all(client_socket, self.length)
        self.raw = message_header + message_payload
        