package safe_socket

import "io"

func SendAll(socket io.Writer, bytes []byte) error {
	bytesWritten := 0
	for bytesWritten < len(bytes) {
		n, err := socket.Write(bytes[bytesWritten:])
		if err != nil {
			return err
		}

		bytesWritten += n
	}

	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	
	bytesRead := 0
	for bytesRead < size {
		n, err := socket.Read(buff[bytesRead:])

		if err != nil {
			return buff, err
		}

		bytesRead += n
	}	

	return buff, nil
}
