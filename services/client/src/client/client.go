package client

import (
	"net"
	"time"

	"os"
	"bufio"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const LENGTH_SIZE = 1
const PAYLOAD_SIZE = 255

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func PackMessage(payload []byte) ([]byte, error) {
	length := len(payload)

	if length > PAYLOAD_SIZE {
		return nil, errors.New("Payload must be lower or equal than 255")
	}

	packet := make([]byte, 1 + length)

	packet[0] = byte(length)
	copy(packet[1:], payload)

	return packet, nil
}

func SendMessage(client *Client, text string, messageArgs []any) error {
	packet, err := PackMessage([]byte(text))
	if err != nil {
		logger.Error("pack-message", logger.Fail, messageArgs...)
		return err
	}

	if err := safe_socket.SendAll(client.conn, packet); err != nil {
		logger.Error("send-message", logger.Fail, messageArgs...)
		return err
	}

	return nil
}

func RecvMessage(client *Client, messageArgs []any) ([]byte, error) {
	header, err := safe_socket.RecvAll(client.conn, LENGTH_SIZE)
	if err != nil {
		logger.Error("recv-message-header", logger.Fail, messageArgs...)
		return nil, err
	}

	length := int(header[0])

	payload, err := safe_socket.RecvAll(client.conn, length)
	if err != nil {
		logger.Error("recv-message-payload", logger.Fail, messageArgs...)
		return nil, err
	}

	return payload, nil
}

func (client *Client) Run() error {
	const mainAction = "upload-lottery-players"
	
	defer client.conn.Close()
	
	readFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "agency-id", client.config.AgencyId, "error", err)
		return err
	}
	
	defer readFile.Close()
	
	writeFile, err := os.OpenFile(client.config.OutputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Error("open-output-file", logger.Fail, "agency-id", client.config.AgencyId, "error", err)
		return err
	}
	
	defer writeFile.Close()

	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)
	
	scanner := bufio.NewScanner(readFile)
	for messageId := 0; scanner.Scan(); messageId++ {
		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
		
		logger.Info(mainAction, logger.InProgress, messageArgs...)

		if err := SendMessage(client, scanner.Text(), messageArgs); err != nil {
			logger.Error("send-message", logger.Fail, messageArgs...)
			return err
		}

		responsePayload, err := RecvMessage(client, messageArgs) 
		if err != nil {
			logger.Error("recv-message", logger.Fail, messageArgs...)
			return err
		}

		_, err = writeFile.WriteString(string(responsePayload) + "\n")
		if err != nil {
			logger.Error("write-file", logger.Fail, messageArgs...)
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-file", logger.Fail, err.Error())
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

	return nil
}
