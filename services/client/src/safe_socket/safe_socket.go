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
		bytesRead += n
		
		// an error occured, could be an EOF
		if err != nil {
			if bytesRead < size {
				return nil, err
			}
		}
	}

	return buff, nil
}
