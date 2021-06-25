package protocol

import (
	"bufio"
	"encoding/binary"
	"io"

	"google.golang.org/protobuf/proto"
)

type Message struct {
	ID         int64
	Entries    []Entry
	Raw        bool
	RawEntries [][]byte
}

func (p *Packet) Read(reader *bufio.Reader) (err error) {
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

func (p *Packet) Write(writer *bufio.Writer) (err error) {
	body, err := proto.Marshal(p)
	if err != nil {
		return
	}
	headBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(headBuf, uint32(len(body)))
	writer.Write(headBuf)
	writer.Write(body)
	return
}
