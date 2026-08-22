import socket
import logger
import safe_socket

_LENGTH_SIZE = 1
_PAYLOAD_SIZE = 255

class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

    def _pack_message(self, payload):
        length = len(payload)

        if length > _PAYLOAD_SIZE:
            raise ValueError(f"Payload must be lower or equal than {_PAYLOAD_SIZE}")

        packet = bytes([length]) + payload
        
        return packet

    def _recv_message(self, client_socket):
        client_message_header, err = safe_socket.recv_all(
            client_socket, _LENGTH_SIZE
        )

        length = client_message_header[0]

        client_message_payload, err = safe_socket.recv_all(
            client_socket, length
        )

        return client_message_payload

    def _send_message(self, client_socket, client_message):
        packet = self._pack_message(client_message)

        safe_socket.send_all(client_socket, packet)

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                client_message = self._recv_message(client_socket)

                if not client_message:
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "messages-amount",
                        message_amount,
                    )
                    return

                message_amount += 1
                
                self._send_message(client_socket, client_message)

        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

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
