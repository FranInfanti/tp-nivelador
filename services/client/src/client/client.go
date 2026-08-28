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
const CONNECTION_ATTEMPS_DELAY_MS = 1000

const HEADER_SIZE = 5
const PAYLOAD_SIZE = 65535
const LINE_SIZE = 256

const (
	OPCODE_DATA	uint8 = 1
	OPCODE_EOF	uint8 = 0
)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	BatchSize  uint8
	AgencyId   uint8
	InputFile  string
	OutputFile string
}

type Client struct {
	conn     net.Conn
	config   ClientConfig
	sendBuff []byte
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{
		conn: conn, 
		config: config,
		sendBuff: make([]byte, HEADER_SIZE + PAYLOAD_SIZE),
	}
	return client, nil
}

func PackMessage(client *Client, opcode uint8, batches uint8, payload []byte) ([]byte, error) {
	if opcode != OPCODE_DATA && opcode != OPCODE_EOF {
		return nil, errors.New("Opcode must exists")
	}
	
	length := len(payload)
	if length > PAYLOAD_SIZE {
		return nil, errors.New("Payload must be lower or equal than 255")
	}

	packet := client.sendBuff[:HEADER_SIZE + length]

	packet[0] = opcode
	packet[1] = batches
	packet[2] = byte(length >> 8) // save the 8 upper bits in 1 byte
	packet[3] = byte(length) // save the 8 lower bits in 1 byte, byte() ignores the 8 upper bits
	packet[4] = client.config.AgencyId
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
	// we take the 8 upper bits and transform it into a 16 bit number and then apply OR with the 8 lower bits
	length := (int(header[2]) << 8) | int(header[3])
	payload, err := safe_socket.RecvAll(client.conn, length)
	if err != nil {
		return 0, 0, nil, err
	}

	return opcode, batches, payload, nil
}

func MakeBatch(batch []byte, payload []byte) ([]byte) {
	batch = append(batch, payload...)
	batch = append(batch, '\n')
	return batch
}

func SendMessage(client *Client, opcode uint8, batches uint8, message []byte) error {
	packet, err := PackMessage(client, opcode, batches, message)
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

	messageId := 0
	inBatch := 0
	batchSize := int(client.config.BatchSize)
	batch := make([]byte, 0, batchSize * LINE_SIZE)

	for scanner.Scan() {
		batch = MakeBatch(batch, scanner.Bytes())
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
		inBatch = 0
		batch = batch[:0]
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
		logger.Error(mainAction, logger.Fail, "agency-id", client.config.AgencyId, "error", err)
		return err
	}
	defer file.Close()

	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)

	if err := readAndSendLotteryPlayers(client, bufio.NewScanner(file)); err != nil {
		logger.Error(mainAction, logger.Fail, err.Error())
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
