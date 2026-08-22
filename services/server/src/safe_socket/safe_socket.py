import socket

def recv_all(socket: socket.socket, size):
    buff = b""
    
    bytes_remaining = size
    while len(buff) < size:
        try: 
            buff += socket.recv(bytes_remaining)
            bytes_remaining = size - len(buff)
        except OSError as err:
            return buff, err

    return buff, None

def send_all(socket: socket.socket, bytes):
    bytes_written = 0

    while bytes_written < len(bytes):
        try:
            n = socket.send(bytes[bytes_written:])
            bytes_written += n
        except OSError as err:
            return err
