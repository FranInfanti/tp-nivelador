import socket
import logger
import safe_socket

from lottery import Lottery, Bet

_HEADER_SIZE = 3
_PAYLOAD_SIZE = 255

_OPCODE_DATA = 1
_OPCODE_EOF = 0

def from_csv_to_bet(agency_id, csv):
    fields = csv.decode("utf-8").strip().split(",")
    
    return Bet(
        agency_id=int(agency_id),
        first_name=fields[0],
        last_name=fields[1],
        document=int(fields[2]),
        birthdate=fields[3],
        number=int(fields[4])
    )

def from_bet_to_csv(bet):
    return bet.agency_id, f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"

class Server:
    def __init__(self, server_host: str, server_port: int, storage_path: str) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(storage_path)

    def _pack_message(self, opcode, agency_id, payload):
        if opcode != _OPCODE_DATA and opcode != _OPCODE_EOF:
            raise ValueError(f"Opcode must exists")

        payload_bytes = payload.encode("utf-8")
        length = len(payload_bytes)

        if length > _PAYLOAD_SIZE:
            raise ValueError(f"Payload must be lower or equal than {_PAYLOAD_SIZE}")

        packet = bytes([opcode]) + bytes([length]) + bytes([agency_id]) + payload_bytes   
        
        return packet

    def _recv_message(self, client_socket):
        client_message_header = safe_socket.recv_all(
            client_socket, _HEADER_SIZE
        )
        
        if not client_message_header:
            return None, None, None

        opcode = client_message_header[0]
        length = client_message_header[1]
        agency_id = client_message_header[2]

        client_message_payload = safe_socket.recv_all(
            client_socket, length
        )

        return opcode, agency_id, client_message_payload

    def _send_message(self, client_socket, opcode, agency_id, client_message):
        packet = self._pack_message(opcode, agency_id, client_message)

        safe_socket.send_all(client_socket, packet)

    def _send_lottery_winners(self, client_socket, agency_id):
        for bet in self.lottery.load_bets():
            if self.lottery.has_won(bet) and bet.agency_id == agency_id:
                _, csv = from_bet_to_csv(bet)
                self._send_message(client_socket, _OPCODE_DATA, agency_id, csv)

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        agency_id = None
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                opcode, agency_id, client_message = self._recv_message(client_socket)

                # the conn has ended abruptly
                if opcode is None:
                    logger.warn(action, logger.LogResult.fail, "messages-amount", message_amount)
                    return

                if opcode == _OPCODE_EOF:
                    logger.info(action, logger.LogResult.success, "messages-amount", message_amount)
                    break

                message_amount += 1
                self.lottery.store_bets([from_csv_to_bet(agency_id, client_message)])

            self._send_lottery_winners(client_socket, agency_id)
            self._send_message(client_socket, _OPCODE_EOF, agency_id, "")
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e
        finally:
            logger.info(action, logger.LogResult.success)
            client_socket.close()

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

                self._handle_client(client_socket)
