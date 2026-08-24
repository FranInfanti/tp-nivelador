import socket

def recv_all(socket: socket.socket, size):
    buff = b""
    
    bytes_remaining = size
    while len(buff) < size:
        try: 
            chunk = socket.recv(bytes_remaining)
            
            # if chunk is 0 bytes, then the connection ended
            if not chunk:
                return b""

            buff += chunk
            bytes_remaining = size - len(buff)
        except OSError as err:
            raise err

    return buff

def send_all(socket: socket.socket, bytes):
    bytes_written = 0

    while bytes_written < len(bytes):
        try:
            n = socket.send(bytes[bytes_written:])
            bytes_written += n
        except OSError as err:
            return err
