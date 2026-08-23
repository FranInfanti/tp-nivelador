package client

import (
	"net"
	"time"

	"os"
	"bufio"
	"errors"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 1000

const HEADER_SIZE = 3
const PAYLOAD_SIZE = 255

const (
	OPCODE_DATA	uint8 = 1
	OPCODE_EOF	uint8 = 0
)

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

func packMessage(opcode uint8, agencyId string, payload []byte) ([]byte, error) {
	if opcode != OPCODE_DATA && opcode != OPCODE_EOF {
		return nil, errors.New("Opcode must exists")
	}
	
	length := len(payload)
	if length > PAYLOAD_SIZE {
		return nil, errors.New("Payload must be lower or equal than 255")
	}

	agencyIdInt, err := strconv.Atoi(agencyId)
	if err != nil || agencyIdInt < 0 || agencyIdInt > 255 {
		return nil, errors.New("AgencyId must be an integer between 0 and 255")
	}

	packet := make([]byte, HEADER_SIZE + length)

	packet[0] = byte(opcode)
	packet[1] = byte(length)
	packet[2] = byte(agencyIdInt)
	copy(packet[HEADER_SIZE:], payload)

	return packet, nil
}

func sendMessage(client *Client, opcode uint8, client_message []byte) error {
	packet, err := packMessage(opcode, client.config.AgencyId, client_message)
	if err != nil {
		return err
	}

	if err := safe_socket.SendAll(client.conn, packet); err != nil {
		return err
	}

	return nil
}

func recvMessage(client *Client) (uint8, []byte, error) {
	header, err := safe_socket.RecvAll(client.conn, HEADER_SIZE)
	if err != nil {
		return 0, nil, err
	}

	opcode := uint8(header[0])
	length := int(header[1])
	_ = int(header[2])

	payload, err := safe_socket.RecvAll(client.conn, length)
	if err != nil {
		return 0, nil, err
	}

	return opcode, payload, nil
}

func sendLotteryPlayers(client *Client) error {
	const mainAction = "send-lottery-players"

	readFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "agency-id", client.config.AgencyId, "error", err)
		return err
	}

	defer readFile.Close()

	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)

	scanner := bufio.NewScanner(readFile)
	for messageId := 0; scanner.Scan(); messageId++ {
		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
		
		logger.Info(mainAction, logger.InProgress, messageArgs...)

		if err := sendMessage(client, OPCODE_DATA, []byte(scanner.Text())); err != nil {
			logger.Error("send-message", logger.Fail, messageArgs...)
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-file", logger.Fail, err.Error())
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

	return nil
}

func sendEOF(client *Client) error {
	const mainAction = "send-eof"
	
	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)

	if err := sendMessage(client, OPCODE_EOF, []byte("")); err != nil {
		logger.Error("send-message", logger.Fail)
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

	return nil
}

func recvLotteryWinners(client *Client) error {
	const mainAction = "recv-lottery-players"

	writeFile, err := os.OpenFile(client.config.OutputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Error("open-output-file", logger.Fail, "agency-id", client.config.AgencyId, "error", err)
		return err
	}
	
	defer writeFile.Close()

	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)

	for {
		opcode, responsePayload, err := recvMessage(client)
		if err != nil {
			logger.Error("recv-message", logger.Fail)
			return err
		}

		if opcode == OPCODE_EOF {
			break
		}

		_, err = writeFile.WriteString(string(responsePayload) + "\n")
		if err != nil {
			logger.Error("write-output-file", logger.Fail)
			return err
		}
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

	return nil
}

func (client *Client) Run() error {
	defer client.conn.Close()
	
	// send the lottery players
	if err := sendLotteryPlayers(client); err != nil {
		return err
	}

	// inform the server there is no more lottery players
	if err := sendEOF(client); err != nil {
		return err
	}

	// await for lottery players winners
	if err := recvLotteryWinners(client); err != nil {
		return err
	}

	return nil
}
