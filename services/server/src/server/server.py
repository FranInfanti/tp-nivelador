import time
import socket
import signal
import logger
import threading
import safe_socket

from types import FrameType
from typing import NoReturn
from lottery import Lottery, Bet
from utils import to_bet, to_csv, Packet

_ENCODING = "utf-8"
_TIMEOUT = 4.0

_HEADER_SIZE = 5
_PAYLOAD_SIZE = 65535

_OPCODE_DATA = 2
_OPCODE_ACK = 1
_OPCODE_EOF = 0

_COLUMNS = 5

class Server:
    def __init__(self, server_host: str, server_port: int, storage_path: str, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.server_socket = None

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
        deadline = time.time() + _TIMEOUT
        
        # wake up the threads waiting for quorum...
        with self.condvar:
            self.condvar.notify_all()
        
        with self.conns_lock:
            for client_socket in list(self.conns.values()):
                # wake up the threads waiting for messages... 
                try: 
                    client_socket.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
            threads = list(self.conns.keys())

        for thread in threads:
            time_left = deadline - time.time()
            if time_left <= 0:
                break
            # wait for the threads to finish...
            thread.join(timeout=time_left)

        with self.conns_lock:
            for client_socket in list(self.conns.values()):
                try:
                    client_socket.close()
                except Exception:
                    pass

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

        logger.info(action, logger.LogResult.success)

    def _store_message(self, packet: Packet):
        batches = packet.payload().strip().split(b'\n')
        bets = []
        i = 0
        for batch in batches:
            i += 1
            bets.append(to_bet(packet.agency_id, batch))

        with self.lottery_lock:
            self.lottery.store_bets(bets)

        if i != packet.batches:
            raise ValueError(f"Batch size mismatch: expected {packet.batches}, got {i}")

    def _send_ack(self, client_socket: socket.socket, packet: Packet):
        packet.pack_message(_OPCODE_ACK, packet.agency_id, str())
        safe_socket.send_all(client_socket, packet.to_bytes())

    def _send_lottery_winners(self, client_socket: socket.socket, packet: Packet, agency_id: int):
        action = "send-lottery-winners"

        logger.info(action, logger.LogResult.in_progress, "ident", threading.get_ident(), "agency-id", packet.agency_id)
    
        with self.lottery_lock:
            bets = self.lottery.load_bets()
        
        for bet in bets:
            if self.lottery.has_won(bet) and bet.agency_id == agency_id:
                csv = to_csv(bet)
                packet.pack_message(_OPCODE_DATA, agency_id, csv)
                safe_socket.send_all(client_socket, packet.to_bytes())

                packet.unpack_message(client_socket)
                if packet.opcode != _OPCODE_ACK:
                    raise ValueError("Expected ACK")

        # there are no more winners then send OEF
        packet.pack_message(_OPCODE_EOF, agency_id, str())
        safe_socket.send_all(client_socket, packet.to_bytes())
        logger.info(action, logger.LogResult.success, "ident", threading.get_ident(), "agency-id", packet.agency_id)

    def _wait_for_quorum(self):
        with self.condvar:
            self.agency_quorum_min -= 1

            while self.agency_quorum_min > 0 and self.running:
                self.condvar.wait()

            self.condvar.notify_all()

    def _close_conn(self, client_socket: socket.socket):
        with self.conns_lock:
            try:
                client_socket.close()
            except Exception:
                pass

            self.conns.pop(threading.current_thread(), None)

    def _handle_client(self, client_socket: socket.socket):
        action = "handle-client"
        message_amount = 0
        packet = Packet()
        try:
            logger.info(action, logger.LogResult.in_progress)
            while self.running:
                packet.unpack_message(client_socket)
                
                args = ["ident", threading.get_ident(), "agency-id", packet.agency_id, "messages-amount", message_amount]

                # the conn ended while the client was still sending data
                if packet.opcode is None:
                    logger.error(action, logger.LogResult.fail, *args)
                    return
                # the client has no more data to send
                elif packet.opcode == _OPCODE_EOF:
                    logger.info(action, logger.LogResult.success, *args)
                    break

                message_amount += 1
                self._store_message(packet)
                self._send_ack(client_socket, packet)
       
            self._wait_for_quorum()
            if self.running:
                self._send_lottery_winners(client_socket, packet, packet.agency_id)
            
            logger.info(action, logger.LogResult.success)
        except Exception as e:
            if self.running:
                args = ["ident", threading.get_ident(), "agency-id", packet.agency_id, "messages-amount", message_amount, "err", e]
                logger.error(action, logger.LogResult.fail, *args)
        finally:
            self._close_conn(client_socket)

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            self.server_socket = server_socket
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while self.running:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    if not self.running:
                        break
                    raise e

                thread = threading.Thread(target=self._handle_client, args=(client_socket,))
                with self.conns_lock:
                    self.conns[thread] = client_socket
                thread.start()
