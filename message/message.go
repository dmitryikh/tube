package message

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/dmitryikh/tube"
)

const (
	lastSupportedVersion = uint8(0)
	maxMessageSize       = 1024 * 1024 * 1024 // 1 GB
)

type Message struct {
	Crc       uint32
	Seq       uint64
	Timestamp uint64
	Payload   []byte
	Meta      map[string][]byte
}

func NewMessage() *Message {
	message := &Message{
		Crc:       0,
		Seq:       0,
		Timestamp: 0,
		Meta:      make(map[string][]byte),
		Payload:   make([]byte, 0),
	}
	return message
}

func (m *Message) Serialize(writer io.Writer) error {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(m.Seq)
	if err != nil {
		return err
	}
	err = encoder.Encode(m.Timestamp)
	if err != nil {
		return err
	}
	err = encoder.Encode(m.Payload)
	if err != nil {
		return err
	}
	err = encoder.Encode(m.Meta)
	if err != nil {
		return err
	}
	crc := tube.DataChecksum(buffer.Bytes())
	m.Crc = crc
	const version = 0
	err = tube.WriteUint32(writer, version)
	if err != nil {
		return err
	}
	err = tube.WriteUint32(writer, crc)
	if err != nil {
		return err
	}
	err = tube.WriteUint32(writer, uint32(len(buffer.Bytes())))
	if err != nil {
		return err
	}
	n, err := writer.Write(buffer.Bytes())
	if err != nil {
		return err
	}
	if n != len(buffer.Bytes()) {
		return fmt.Errorf("write only %d bytes", len(buffer.Bytes()))
	}
	return nil
}

func (m *Message) Deserialize(reader io.Reader, checkCRC bool) error {
	// first 4 bytes - reserved for version
	version, err := tube.ReadUint32(reader)
	if err != nil {
		return err
	}
	if version == 0 {
		crc, err := tube.ReadUint32(reader)
		if err != nil {
			return err
		}
		length, err := tube.ReadUint32(reader)
		if err != nil {
			return err
		}
		if length > maxMessageSize {
			return fmt.Errorf("can't read large message (%d bytes). Limit %d", length, maxMessageSize)
		}
		buffer := make([]byte, length)
		n, err := reader.Read(buffer)
		if uint32(n) != length {
			return fmt.Errorf("can't read buffer of %d bytes (got %d bytes)", length, n)
		}
		if err != nil {
			return err
		}

		if checkCRC {
			crcActual := tube.DataChecksum(buffer)
			if crcActual != crc {
				return fmt.Errorf("wrong checksum")
			}
		}
		m.Crc = crc
		readBuffer := bytes.NewBuffer(buffer)
		decoder := gob.NewDecoder(readBuffer)
		err = decoder.Decode(&m.Seq)
		if err != nil {
			return err
		}
		err = decoder.Decode(&m.Timestamp)
		if err != nil {
			return err
		}
		err = decoder.Decode(&m.Payload)
		if err != nil {
			return err
		}
		err = decoder.Decode(&m.Meta)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unsupported version %d", version)
	}
	return nil
}
