package client

import (
	"net"
	"time"

	"os"
	"bufio"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const ECHO_CLIENT_BUFFER_SIZE = 512
const ECHO_CLIENT_MESSAGE_AMOUNT = 3
const ECHO_CLIENT_MESSAGE_DELAY_MS = 1000

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

func (client *Client) Run() error {
	const mainAction = "test-echo-server"

	// when the function ends, close the conn
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

	scanner := bufio.NewScanner(readFile)

	for messageId := 0; scanner.Scan(); messageId++ {
		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
		logger.Info(mainAction, logger.InProgress, messageArgs...)

		clientMessage := scanner.Text()
		
		if err := safe_socket.SendAll(client.conn, []byte(clientMessage)); err != nil {
			logger.Error("send-message", logger.Fail, messageArgs...)
			return err
		}
	
		responseBuffer, err := safe_socket.RecvAll(client.conn, ECHO_CLIENT_BUFFER_SIZE)
		if err != nil {
			logger.Error("recv-response", logger.Fail, messageArgs...)
			return err
		}
	
		if string(responseBuffer) == clientMessage {
			logger.Error("check-response", logger.Fail, messageArgs...)
			return err
		}

		_, err = writeFile.WriteString(string(responseBuffer) + "\n")
		if err != nil {
			logger.Error("write-file", logger.Fail, messageArgs...)
			return err
		}

		time.Sleep(ECHO_CLIENT_MESSAGE_DELAY_MS * time.Millisecond)
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-file", logger.Fail, err.Error())
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

	return nil
}
