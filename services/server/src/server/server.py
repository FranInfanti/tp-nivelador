import sys
import socket
import signal
import logger
import threading
import safe_socket

from types import FrameType
from typing import NoReturn
from lottery import Lottery, Bet

_ENCODING = "utf-8"
_TIMEOUT = 5.0

_HEADER_SIZE = 5
_PAYLOAD_SIZE = 65535

_OPCODE_DATA = 1
_OPCODE_EOF = 0

_COLUMNS = 5

def to_bet(agency_id: int, csv: bytes) -> Bet:
    fields = csv.decode(_ENCODING).strip().split(",")
    
    return Bet(
        agency_id=agency_id,
        first_name=fields[0],
        last_name=fields[1],
        document=int(fields[2]),
        birthdate=fields[3],
        number=int(fields[4])
    )

def to_csv(bet: Bet) -> str:
    return f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"

def pack_message(opcode: int, agency_id: int, payload: str) -> bytes:
    if opcode not in [_OPCODE_DATA, _OPCODE_EOF]:
        raise ValueError(f"Opcode must be valid")

    payload_bytes = payload.encode(_ENCODING)
    length = len(payload_bytes)

    if length > _PAYLOAD_SIZE:
        raise ValueError(f"Payload must be lower or equal than {_PAYLOAD_SIZE}")
    
    length_high = (length >> 8) & 0xFF
    length_low = length & 0xFF

    return bytes([opcode, 1, length_high, length_low, agency_id]) + payload_bytes

def unpack_message(client_socket: socket.socket) -> (int, int, int, bytes):
    message_header = safe_socket.recv_all(client_socket, _HEADER_SIZE)
    
    if not message_header:
        return None, None, None, None

    opcode = int(message_header[0])
    batches = int(message_header[1])
    length = (int(message_header[2]) << 8) | int(message_header[3])
    agency_id = int(message_header[4])
    message_payload = safe_socket.recv_all(client_socket, length)

    return opcode, batches, agency_id, message_payload     

class Server:
    def __init__(self, server_host: str, server_port: int, storage_path: str, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

        self.conns = {}
        self.conns_lock = threading.Lock()
        
        self.lottery = Lottery(storage_path)
        self.lottery_lock = threading.Lock()
        
        self.agency_quorum_min = agency_quorum_min
        self.running = True
        self.condvar = threading.Condition()

        signal.signal(signal.SIGTERM, self._sigterm_handler)

    def _sigterm_handler(self, _: int, frame: FrameType | None) -> NoReturn:
        action = "sigterm-received"
        logger.info(action, logger.LogResult.in_progress)
        
        self.running = False
        
        with self.condvar:
            self.condvar.notify_all()
        
        with self.conns_lock:
            for client_socket in list(self.conns.values()):
                # if the socket is already closed, there is no issue
                try: 
                    client_socket.shutdown(socket.SHUT_RDWR)
                    client_socket.close()
                except Exception:
                    pass
            
            threads = list(self.conns.keys())

        for thread in threads:
            thread.join(timeout=_TIMEOUT)
        
        logger.info(action, logger.LogResult.success)
        sys.exit(0)

    def _store_message(self, batch_size: int, agency_id: str, message: bytes):
        batches = message.strip().split(b'\n')
        bets = []
        i = 0
        for batch in batches:
            i += 1
            bets.append(to_bet(agency_id, batch))

        with self.lottery_lock:
            self.lottery.store_bets(bets)

        if i != batch_size:
            raise ValueError(f"Batch size mismatch: expected {batch_size}, got {i}")

    def _send_lottery_winners(self, client_socket: socket.socket, agency_id: int):
        with self.lottery_lock:
            bets = self.lottery.load_bets()
        
        for bet in bets:
            if self.lottery.has_won(bet) and bet.agency_id == agency_id:
                csv = to_csv(bet)
                packet = pack_message(_OPCODE_DATA, agency_id, csv)
                safe_socket.send_all(client_socket, packet)

        # there are no more winners then send OEF
        packet = pack_message(_OPCODE_EOF, agency_id, str())
        safe_socket.send_all(client_socket, packet)

    def _wait_for_quorum(self):
        with self.condvar:
            self.agency_quorum_min -= 1

            while self.agency_quorum_min > 0 and self.running:
                self.condvar.wait()

            self.condvar.notify_all()

    def _handle_client(self, client_socket: socket.socket):
        action = "handle-client"
        message_amount = 0
        agency_id = None
        try:
            logger.info(action, logger.LogResult.in_progress)
            while self.running:
                opcode, batch_size, agency_id, client_message = unpack_message(client_socket)

                # the conn ended while the client was still sending data
                if opcode is None:
                    logger.error(action, logger.LogResult.fail, "messages-amount", message_amount)
                    return
                # the client has no more data to send
                elif opcode == _OPCODE_EOF:
                    logger.info(action, logger.LogResult.success, "messages-amount", message_amount)
                    break

                message_amount += 1
                self._store_message(batch_size, agency_id, client_message)

            self._wait_for_quorum()
            if self.running:
                self._send_lottery_winners(client_socket, agency_id)
        except Exception as e:
            logger.error(action, logger.LogResult.fail, "messages-amount", message_amount)
            raise e
        finally:
            logger.info(action, logger.LogResult.success)
            
            try: 
                client_socket.shutdown(socket.SHUT_RDWR)
                client_socket.close()
            except:
                pass

            with self.conns_lock:
                self.conns.pop(threading.current_thread(), None)

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                thread = threading.Thread(target=self._handle_client, args=(client_socket,))
                
                with self.conns_lock:
                    self.conns[thread] = client_socket
                
                thread.start()
