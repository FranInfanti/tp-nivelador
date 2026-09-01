import socket
import safe_socket

_HEADER_SIZE = 5
_PAYLOAD_SIZE = 65535

OPCODE_DATA = 2
OPCODE_ACK = 1
OPCODE_EOF = 0

_ENCODING = "utf-8"

class Packet:
    def __init__(self, opcode: int, batches: int = 1, agency_id: int = 0, payload: bytes | str = b""):
        if opcode not in [OPCODE_DATA, OPCODE_ACK, OPCODE_EOF]:
            raise ValueError("Opcode must be valid")

        if isinstance(payload, str):
            payload_bytes = payload.encode(_ENCODING)
        else:
            payload_bytes = payload

        length = len(payload_bytes)
        if length > _PAYLOAD_SIZE:
            raise ValueError(f"Payload must be lower or equal than {_PAYLOAD_SIZE}")

        self.opcode = opcode
        self.batches = batches
        self.length = length
        self.agency_id = agency_id
        self.payload = payload_bytes

    def to_bytes(self) -> bytes:
        length_high = (self.length >> 8) & 0xFF
        length_low = self.length & 0xFF

        header = bytes([self.opcode, self.batches, length_high, length_low, self.agency_id])
        return header + self.payload

    @classmethod
    def from_socket(cls, client_socket: socket.socket) -> "Packet | None":
        message_header = safe_socket.recv_all(client_socket, _HEADER_SIZE)
        if not message_header:
            return None

        opcode = int(message_header[0])
        batches = int(message_header[1])
        length = (int(message_header[2]) << 8) | int(message_header[3])
        agency_id = int(message_header[4])

        message_payload = safe_socket.recv_all(client_socket, length)

        return cls(opcode=opcode, batches=batches, agency_id=agency_id, payload=message_payload)        