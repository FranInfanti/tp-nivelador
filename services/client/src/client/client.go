package client

import (
	"net"
	"time"

	"os"
	"bufio"
	"errors"
	"syscall"
	"context"
	"os/signal"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 1000

const HEADER_SIZE = 5
const PAYLOAD_SIZE = 65535
const LINE_SIZE = 256

const (
	OPCODE_DATA	uint8 = 2
	OPCODE_ACK  uint8 = 1
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

func uploadBatch(client *Client, inBatch uint8, batch []byte) error {
	if err := SendMessage(client, OPCODE_DATA, inBatch, batch); err != nil {
		return err
	}

	// wait for ack
	opcode, _, _, err := UnpackMessage(client)
	if err != nil {
		return err
	}

	if opcode != OPCODE_ACK {
		return errors.New("Expected ACK from server")
	}

	return nil
}

func uploadLotteryPlayers(client *Client, scanner *bufio.Scanner) error {
	const action = "upload-lottery-players"
	
	messageId := 0
	inBatch := 0
	batchSize := int(client.config.BatchSize)
	batch := make([]byte, 0, batchSize * LINE_SIZE)
	for scanner.Scan() {
		batch = MakeBatch(batch, scanner.Bytes())
		inBatch++

		if inBatch == batchSize {
			if err := uploadBatch(client, uint8(inBatch), batch); err != nil {
				return err
			}

			messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId, "batches", inBatch}
			logger.Info(action, logger.Success, messageArgs...)
	
			messageId++
			inBatch = 0
			batch = batch[:0]
		}
	}

	if inBatch > 0 {
		if err := uploadBatch(client, uint8(inBatch), batch); err != nil {
			return err
		}

		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId, "batches", inBatch}
		logger.Info(action, logger.Success, messageArgs...)
	}

	return scanner.Err()
}

func downloadLotteryWinners(client *Client, writeFile *os.File) error {
	const action = "download-lottery-winners"

	messageId := 0
	for {
		opcode, _, message, err := UnpackMessage(client)
		if err != nil {
			return err
		}

		// this is not an error, cause it means there is no more winners
		if opcode == OPCODE_EOF {
			break
		}

		messageId++
		message = append(message, '\n')
		_, err = writeFile.Write(message)
		if err != nil {
			return err
		}

		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
		logger.Info(action, logger.Success, messageArgs...)
	}

	return nil
}

func sendData(client *Client) error {
	logger.Info("send-data", logger.InProgress)

	file, err := os.Open(client.config.InputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := uploadLotteryPlayers(client, bufio.NewScanner(file)); err != nil {
		return err
	}

	logger.Info("send-data", logger.Success)
	return nil
}

func sendEOF(client *Client) error {
	logger.Info("send-eof", logger.InProgress)

	if err := SendMessage(client, OPCODE_EOF, 0, []byte{}); err != nil {
		return err
	}

	logger.Info("send-eof", logger.Success)
	return nil
}

func recvData(client *Client) error {
	logger.Info("recv-data", logger.InProgress)

	file, err := os.OpenFile(client.config.OutputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := downloadLotteryWinners(client, file); err != nil {
		return err
	}

	logger.Info("recv-data", logger.Success)
	return nil
}

func (client *Client) Run() error {
	defer client.conn.Close()
	
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	go func() {
		// wait for SIGTERM signal and end the conn...
		<-ctx.Done()
		logger.Warn("sigterm-signal", logger.InProgress)
		client.conn.Close()
	}()

	if err := sendData(client); err != nil {
		if ctx.Err() != nil {
			return nil
		}

		logger.Error("send-data", logger.Fail, "err", err)
		return err
	}

	if err := sendEOF(client); err != nil {
		if ctx.Err() != nil {
			return nil
		}

		logger.Error("send-eof", logger.Fail, "err", err)
		return err
	}

	if err := recvData(client); err != nil {
		if ctx.Err() != nil {
			return nil
		}

		logger.Error("recv-data", logger.Fail, "err", err)
		return err
	}

	return nil
}
