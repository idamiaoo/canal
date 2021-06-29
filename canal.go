package canal

import (
	"bufio"
	"encoding/hex"
	"errors"
	"net"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/go-canal/canal/protocol"
)

var (
	ErrUnsupportedVersion = errors.New("unsupported version at this client")
	ErrExpectHandshake    = errors.New("expect handshake but found other type")
	ErrUnexpectedPacket   = errors.New("unexpected packet type when ack is expected")
)

type Client struct {
	opt        *Options
	netConn    net.Conn
	readerPool sync.Pool
	packetPool sync.Pool
	writerPool sync.Pool
	connected  bool
}

func NewClient(opt *Options) *Client {
	opt.init()
	return &Client{
		opt: opt,
	}
}

func (c *Client) initConn() error {
	conn, err := net.DialTimeout("tcp", c.opt.Address, c.opt.DialTimeout)
	if err != nil {
		return err
	}
	reader := c.readerPool.Get().(*bufio.Reader)
	reader.Reset(conn)
	p := c.packetPool.Get().(*protocol.Packet)
	if err := p.Read(reader); err != nil {
		return err
	}

	if p.GetVersion() != 1 {
		return ErrUnsupportedVersion
	}

	if p.GetType() != protocol.PacketType_HANDSHAKE {
		return ErrExpectHandshake
	}

	var handshake protocol.Handshake
	if err := proto.Unmarshal(p.GetBody(), &handshake); err != nil {
		return err
	}

	seed := handshake.GetSeeds()
	newPasswd := c.opt.Password
	if newPasswd != "" {
		newPasswd = hex.EncodeToString(Scramble411([]byte(newPasswd), seed))
	}
	clientAuth := &protocol.ClientAuth{
		Username:               c.opt.Username,
		Password:               []byte(newPasswd),
		NetReadTimeoutPresent:  &protocol.ClientAuth_NetReadTimeout{NetReadTimeout: int32(c.opt.ReadTimeout.Seconds())},
		NetWriteTimeoutPresent: &protocol.ClientAuth_NetWriteTimeout{NetWriteTimeout: int32(c.opt.ReadTimeout.Seconds())},
	}
	rawClientAuth, _ := proto.Marshal(clientAuth)
	p = &protocol.Packet{
		Type: protocol.PacketType_CLIENTAUTHENTICATION,
		Body: rawClientAuth,
	}
	writer := c.writerPool.Get().(*bufio.Writer)
	if err := p.Write(writer); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	p.Reset()
	if err := p.Read(reader); err != nil {
		return err
	}
	if p.GetType() != protocol.PacketType_ACK {
		return ErrUnexpectedPacket
	}
	var ack protocol.Ack
	if err := proto.Unmarshal(p.GetBody(), &ack); err != nil {
		return err
	}
	if ack.GetErrorCode() > 0 {
		return errors.New("something goes wrong when doing authentication: " + ack.GetErrorMessage())
	}
	c.connected = true
	return nil
}

func (c *Client) Subscribe(filter string) error {
	return nil
}
