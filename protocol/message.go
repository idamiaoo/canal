package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

type Message struct {
	ID         int64
	Entries    []Entry
	Raw        bool
	RawEntries [][]byte
}

func (p *Packet) ParseMessage(lazyParseEntry bool) (*Message, error) {
	if p == nil {
		return nil, nil
	}
	switch p.GetType() {
	case PacketType_MESSAGES:
		compression, ok := p.CompressionPresent.(*Packet_Compression)
		if !ok {
			return nil, errors.New("compression is not supported in this connector")
		}
		if compression.Compression != Compression_NONE && compression.Compression != Compression_COMPRESSIONCOMPATIBLEPROTO2 {
			return nil, errors.New("compression is not supported in this connector")
		}

		var messages Messages
		if err := proto.Unmarshal(p.GetBody(), &messages); err != nil {
			return nil, fmt.Errorf("something goes wrong with reason: %v", err)
		}

		message := &Message{
			ID: messages.GetBatchId(),
		}

		if lazyParseEntry {
			message.Raw = true
			message.RawEntries = messages.GetMessages()
		} else {
			message.Raw = false
			for _, v := range messages.GetMessages() {
				var entry Entry
				if err := proto.Unmarshal(v, &entry); err != nil {
					return nil, fmt.Errorf("something goes wrong with reason: %v", err)
				}
				message.Entries = append(message.Entries, entry)
			}
		}
		return message, nil
	case PacketType_ACK:
		var ack Ack
		if err := proto.Unmarshal(p.GetBody(), &ack); err != nil {
			return nil, fmt.Errorf("something goes wrong with reason: %v", err)
		}
		return nil, fmt.Errorf("something goes wrong with reason: %s", ack.GetErrorMessage())
	default:
		return nil, fmt.Errorf("unexpected packet type: %d", p.GetType())
	}
}

func (p *Packet) Read(reader io.Reader) (err error) {
	headBuf := make([]byte, 4)
	_, err = io.ReadFull(reader, headBuf)
	if err != nil {
		return
	}
	bodyLen := int(binary.BigEndian.Uint32(headBuf))
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return
	}
	err = proto.Unmarshal(body, p)
	if err != nil {
		return err
	}
	return
}

func (p *Packet) Write(writer io.Writer) (err error) {
	var body []byte
	body, err = proto.Marshal(p)
	if err != nil {
		return
	}
	headBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(headBuf, uint32(len(body)))
	_, err = writer.Write(headBuf)
	if err != nil {
		return
	}
	_, err = writer.Write(body)
	return
}
