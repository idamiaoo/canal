package canal

import (
	"errors"
	"fmt"

	"github.com/katakurin/canal/protobuf/entry"
	"github.com/katakurin/canal/protobuf/protocol"

	"google.golang.org/protobuf/proto"
)

type Message struct {
	ID         int64
	Entries    []entry.Entry
	Raw        bool
	RawEntries [][]byte
}

func ParseMessage(p *protocol.Packet, lazyParseEntry bool) (*Message, error) {
	if p == nil {
		return nil, nil
	}
	switch p.GetType() {
	case protocol.PacketType_MESSAGES:
		fmt.Println(p.GetCompression())
		if p.GetCompression() != protocol.Compression_NONE && p.GetCompression() != protocol.Compression_COMPRESSIONCOMPATIBLEPROTO2 {
			return nil, errors.New("compression is not supported in this connector")
		}

		var messages protocol.Messages
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
			entries := make([]entry.Entry, 0, len(messages.GetMessages()))
			for _, v := range messages.GetMessages() {
				var entry entry.Entry
				if err := proto.Unmarshal(v, &entry); err != nil {
					return nil, fmt.Errorf("something goes wrong with reason: %v", err)
				}
				entries = append(entries, entry)
			}
			message.Raw = false
			message.Entries = entries
		}
		return message, nil
	case protocol.PacketType_ACK:
		var ack protocol.Ack
		if err := proto.Unmarshal(p.GetBody(), &ack); err != nil {
			return nil, fmt.Errorf("something goes wrong with reason: %v", err)
		}
		return nil, fmt.Errorf("something goes wrong with reason: %s", ack.GetErrorMessage())
	default:
		return nil, fmt.Errorf("unexpected packet type: %d", p.GetType())
	}
}
