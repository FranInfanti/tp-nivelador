package packet

import (
	"net"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const ( 
	HEADER_SIZE  int = 5
	PAYLOAD_SIZE int = 65535
)

const (
	OPCODE_DATA	uint8 = 2
	OPCODE_ACK  uint8 = 1
	OPCODE_EOF	uint8 = 0
)

type Packet struct {
	// Type of packet
	Opcode   byte
	// Amount of batches sended
	Batches  byte
	// Payload length
	Length   uint16
	// ID of the agency sending the packet
	AgencyId byte
	// Raw bytes containing the packet
	raw      []byte
}

// Returns the memory address from an empty packet
// 
func NewPacket() *Packet {
	return &Packet{
		raw: make([]byte, HEADER_SIZE + PAYLOAD_SIZE),
	}
}

// Returns the raw bytes containing the packet
// 
func (packet *Packet) ToBytes() []byte {
	return packet.raw[:HEADER_SIZE + int(packet.Length)]
}

// Returns the payload contained by the packet
// 
func (packet *Packet) Payload() []byte {
    return packet.raw[HEADER_SIZE : HEADER_SIZE + int(packet.Length)]
}

// Modifys the packet with the information provided
// 
func PackMessage(packet *Packet, opcode byte, batches byte, agencyId byte, payload []byte) error {
	if opcode != OPCODE_DATA && opcode != OPCODE_EOF && opcode != OPCODE_ACK {
		return errors.New("Invalid opcode")
	}

	length := len(payload)
	if length > PAYLOAD_SIZE {
		return errors.New("Payload must be lower or equal than 65535")
	}
	
	packet.Opcode = opcode
	packet.Batches = batches
	packet.Length = uint16(length)
	packet.AgencyId = agencyId

	packet.raw[0] = opcode
	packet.raw[1] = batches
	packet.raw[2] = byte(length >> 8)
	packet.raw[3] = byte(length)
	packet.raw[4] = agencyId
	copy(packet.raw[HEADER_SIZE:], payload)

	return nil
}

// Reads from the connection the packet that has been 
// received and saves it in the packet
// 
func UnpackMessage(packet *Packet, conn net.Conn) error {
	header, err := safe_socket.RecvAll(conn, HEADER_SIZE)
	if err != nil {
		return err
	}

	packet.Opcode = header[0]
	packet.Batches = header[1]
	packet.Length = (uint16(header[2]) << 8) | uint16(header[3])
	packet.AgencyId = header[4]
	copy(packet.raw[:HEADER_SIZE], header)

	payload, err := safe_socket.RecvAll(conn, int(packet.Length))
	if err != nil {
		return err
	}
	copy(packet.raw[HEADER_SIZE:], payload)

	return nil
}
