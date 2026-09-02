package client

import (
	"os"
	"net"
	"time"
	"bufio"
	"errors"
	"syscall"
	"context"
	"os/signal"

	packet "github.com/7574-sistemas-distribuidos/tp-nivelador/src/utils"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 1000

const LINE_SIZE = 256

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputFile  string
	OutputFile string
	BatchSize  uint8
	AgencyId   uint8
}

type Client struct {
	conn   net.Conn
	config ClientConfig
	packet *packet.Packet
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, err
	}

	client := &Client{
		conn: conn, 
		config: config,
		packet: packet.NewPacket(),
	}
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

func (client *Client) sendMessage(opcode uint8, batches uint8, message []byte) error {
	err := client.packet.PackMessage(opcode, batches, client.config.AgencyId, message)
	if err != nil {
		return err
	}

	if err := safe_socket.SendAll(client.conn, client.packet.ToBytes()); err != nil {
		return err
	}

	return nil
}

func (client *Client) uploadBatch(inBatch uint8, batch []byte) error {
	if err := client.sendMessage(packet.OPCODE_DATA, inBatch, batch); err != nil {
		return err
	}

	// wait for ack
	if err := client.packet.UnpackMessage(client.conn); err != nil {
		return err
	}

	if client.packet.Opcode != packet.OPCODE_ACK {
		return errors.New("Expected ACK from server")
	}

	return nil
}

func (client *Client) uploadLotteryPlayers(scanner *bufio.Scanner) error {
	const action = "upload-lottery-players"
	
	messageId := 0
	inBatch := 0
	batchSize := int(client.config.BatchSize)
	batch := make([]byte, 0, batchSize * LINE_SIZE)
	for scanner.Scan() {
		batch = append(batch, scanner.Bytes()...)
		batch = append(batch, '\n')
		inBatch++

		if inBatch == batchSize {
			if err := client.uploadBatch(uint8(inBatch), batch); err != nil {
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
		if err := client.uploadBatch(uint8(inBatch), batch); err != nil {
			return err
		}

		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId, "batches", inBatch}
		logger.Info(action, logger.Success, messageArgs...)
	}

	return scanner.Err()
}

func (client *Client) downloadLotteryWinners(writeFile *os.File) error {
    const action = "download-lottery-winners"

    messageId := 0
    for {
        if err := client.packet.UnpackMessage(client.conn); err != nil {
            return err
        }

        if client.packet.Opcode == packet.OPCODE_EOF {
            break
        }
        
		messageId++
        message := append(client.packet.Payload(), '\n')
        
		_, err := writeFile.Write(message)
        if err != nil {
            return err
        }

		if err := client.sendMessage(packet.OPCODE_ACK, 0, []byte{}); err != nil {
			return err
		}
        
		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
        logger.Info(action, logger.Success, messageArgs...)
    }

    return nil
}

func (client *Client) sendData() error {
	logger.Info("send-data", logger.InProgress)

	file, err := os.Open(client.config.InputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := client.uploadLotteryPlayers(bufio.NewScanner(file)); err != nil {
		return err
	}

	logger.Info("send-data", logger.Success)
	return nil
}

func (client *Client) sendEOF() error {
	logger.Info("send-eof", logger.InProgress)

	if err := client.sendMessage(packet.OPCODE_EOF, 0, []byte{}); err != nil {
		return err
	}

	logger.Info("send-eof", logger.Success)
	return nil
}

func (client *Client) recvData() error {
	logger.Info("recv-data", logger.InProgress)

	file, err := os.OpenFile(client.config.OutputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := client.downloadLotteryWinners(file); err != nil {
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

	if err := client.sendData(); err != nil {
		if ctx.Err() != nil {
			return nil
		}

		logger.Error("send-data", logger.Fail, "err", err)
		return err
	}

	if err := client.sendEOF(); err != nil {
		if ctx.Err() != nil {
			return nil
		}

		logger.Error("send-eof", logger.Fail, "err", err)
		return err
	}

	if err := client.recvData(); err != nil {
		if ctx.Err() != nil {
			return nil
		}

		logger.Error("recv-data", logger.Fail, "err", err)
		return err
	}

	return nil
}
