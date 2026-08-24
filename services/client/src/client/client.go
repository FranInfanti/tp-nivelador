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

const HEADER_SIZE = 4
const PAYLOAD_SIZE = 255

const COLUMNS = 5

const (
	OPCODE_DATA	uint8 = 1
	OPCODE_EOF	uint8 = 0
)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	BatchSize  string
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

func PackMessage(opcode uint8, batches uint8, agencyId string, payload []byte) ([]byte, error) {
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
	packet[1] = byte(batches)
	packet[2] = byte(length)
	packet[3] = byte(agencyIdInt)
	copy(packet[HEADER_SIZE:], payload)

	return packet, nil
}

func UnpackMessage(client *Client) (uint8, uint8, []byte, error) {
	header, err := safe_socket.RecvAll(client.conn, HEADER_SIZE)
	if err != nil {
		return 0, 0, nil, err
	}

	opcode := uint8(header[0])
	batches := uint8(header[1])
	length := int(header[2])

	payload, err := safe_socket.RecvAll(client.conn, length)
	if err != nil {
		return 0, 0, nil, err
	}

	return opcode, batches, payload, nil
}

func SendMessage(client *Client, opcode uint8, batches uint8, message []byte) error {
	packet, err := PackMessage(opcode, batches, client.config.AgencyId, message)
	if err != nil {
		return err
	}

	if err := safe_socket.SendAll(client.conn, packet); err != nil {
		return err
	}

	return nil
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

func readAndSendLotteryPlayers(client *Client, scanner *bufio.Scanner) error {
	const mainAction = "send-lottery-players"
	
	batchSize, err := strconv.Atoi(client.config.BatchSize)
	if err != nil {
		return errors.New("Invalid batch size configuration")
	}
	
	messageId := 0
	batch, inBatch := []byte{}, 0
	for scanner.Scan() {
		scanner_bytes := scanner.Bytes()
		length := len(scanner_bytes)
    	batch = append(batch, byte(length))
    	batch = append(batch, scanner_bytes...)
		inBatch++

		if inBatch < batchSize {
			continue
		}

		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId, "batches", inBatch}
		logger.Info(mainAction, logger.InProgress, messageArgs...)

		if err := SendMessage(client, OPCODE_DATA, uint8(inBatch), batch); err != nil {
			logger.Error("send-message", logger.Fail, messageArgs...)
			return err
		}

		messageId++
		batch, inBatch = []byte{}, 0
	}

	if inBatch > 0 {
		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId, "batches", inBatch}
		logger.Info(mainAction, logger.InProgress, messageArgs...)
		
		if err := SendMessage(client, OPCODE_DATA, uint8(inBatch), batch); err != nil {
			logger.Error("send-message", logger.Fail, messageArgs...)
			return err
		}
	}

	return scanner.Err()
}

func recvAndSaveLotteryWinners(client *Client, writeFile *os.File) error {
	for {
		opcode, _, message, err := UnpackMessage(client)
		if err != nil {
			logger.Error("recv-message", logger.Fail)
			return err
		}

		if opcode == OPCODE_EOF {
			break
		}

		message = append(message, '\n')

		_, err = writeFile.Write(message)
		if err != nil {
			logger.Error("write-output-file", logger.Fail)
			return err
		}
	}

	return nil
}

func sendLotteryPlayers(client *Client) error {
	const mainAction = "send-lottery-players"

	file, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-file", logger.Fail, "agency-id", client.config.AgencyId, "error", err)
		return err
	}
	defer file.Close()

	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)

	if err := readAndSendLotteryPlayers(client, bufio.NewScanner(file)); err != nil {
		logger.Error("read-file", logger.Fail, err.Error())
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)
	return nil
}

func sendEOF(client *Client) error {
	const mainAction = "send-eof"
	
	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)

	if err := SendMessage(client, OPCODE_EOF, 0, []byte{}); err != nil {
		logger.Error("send-message", logger.Fail)
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)
	return nil
}

func recvLotteryWinners(client *Client) error {
	const mainAction = "recv-lottery-winners"

	file, err := os.OpenFile(client.config.OutputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Error("open-file", logger.Fail, "agency-id", client.config.AgencyId, "error", err)
		return err
	}
	defer file.Close()

	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)

	if err := recvAndSaveLotteryWinners(client, file); err != nil {
		logger.Error(mainAction, logger.Fail, "agency-id", client.config.AgencyId)
		return err
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
