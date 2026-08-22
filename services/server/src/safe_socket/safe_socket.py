import socket

def recv_all(socket: socket.socket, size):
    buff = b""
    
    bytesRemaining = size
    while len(buff) < size:
        try: 
            buff += socket.recv(bytesRemaining)
            bytesRemaining = size - len(buff)
        except OSError as err:
            return buff, err

    return buff, None

def send_all(socket: socket.socket, bytes):
    bytesWritten = 0

    while bytesWritten < len(bytes):
        try:
            n = socket.send(bytes[bytesWritten:])
            bytesWritten += n
        except OSError as err:
            return err
