package protocol

import (
	"encoding/binary"
	"io"

	"google.golang.org/protobuf/proto"
)

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
