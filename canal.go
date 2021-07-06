package canal

import (
	"encoding/hex"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/go-canal/canal/protocol"
)

var (
	ErrUnsupportedVersion = errors.New("unsupported version at this client")
	ErrExpectHandshake    = errors.New("expect handshake but found other type")
	ErrUnexpectedPacket   = errors.New("unexpected packet type when ack is expected")
)

type Client struct {
	opts            clientOptions
	addr            string // host:port address.
	username        string
	password        string
	netConn         net.Conn
	readableChannel chan *protocol.Packet
	writableChannel chan *protocol.Packet
	connected       uint32
	clientIdentity  clientIdentity
	mutex           sync.Mutex
	closeSingle     chan struct{}
}

type clientIdentity struct {
	destination string
	clientId    int
	filter      string
}

func NewClient(addr, username, password string, opts ...ClientOption) (*Client, error) {
	cc := &Client{
		opts: defaultClientOptions(),
		addr: addr,
	}

	for _, opt := range opts {
		opt.apply(&cc.opts)
	}
	cc.clientIdentity = clientIdentity{
		clientId:    cc.opts.clientID,
		destination: cc.opts.destination,
	}

	if err := cc.handshake(); err != nil {
		return nil, err
	}
	return cc, nil
}

func (c *Client) connect() error {
	conn, err := net.DialTimeout("tcp", c.addr, c.opts.dialTimeout)
	if err != nil {
		return err
	}

	c.netConn = conn
	c.readableChannel = make(chan *protocol.Packet, 64)
	c.writableChannel = make(chan *protocol.Packet, 64)

	go func() {
		for {
			select {
			case <-c.closeSingle:
				return
			default:

			}
			var packet protocol.Packet
			if err := packet.Read(c.netConn); err != nil {
				c.disconnect()
			}
			c.readableChannel <- &packet
		}
	}()

	go func() {
		for {
			select {
			case packet := <-c.readableChannel:
				if err := packet.Write(c.netConn); err != nil {
					c.disconnect()
				}
			case <-c.closeSingle:
				return
			}
		}
	}()

	return nil
}

func (c *Client) Disconnect() error {
	return c.disconnect()
}

func (c *Client) disconnect() error {
	if c.opts.rollbackOnDisConnect && c.connected == 1 {
		if err := c.Rollback(0); err != nil {
			return err
		}
	}
	_ = c.netConn.Close()
	c.connected = 0
	return nil
}

func (c *Client) handshake() error {
	packet := &protocol.Packet{}
	if err := c.readPacket(packet); err != nil {
		return err
	}
	if packet.GetVersion() != 1 {
		return ErrUnsupportedVersion
	}

	if packet.GetType() != protocol.PacketType_HANDSHAKE {
		return ErrExpectHandshake
	}
	var handshake protocol.Handshake
	if err := proto.Unmarshal(packet.GetBody(), &handshake); err != nil {
		return err
	}
	seed := handshake.GetSeeds()
	newPasswd := c.password
	if newPasswd != "" {
		newPasswd = hex.EncodeToString(Scramble411([]byte(newPasswd), seed))
	}
	clientAuth := &protocol.ClientAuth{
		Username:               c.username,
		Password:               []byte(newPasswd),
		NetReadTimeoutPresent:  &protocol.ClientAuth_NetReadTimeout{NetReadTimeout: int32(c.opts.readTimeout.Seconds())},
		NetWriteTimeoutPresent: &protocol.ClientAuth_NetWriteTimeout{NetWriteTimeout: int32(c.opts.writeTimeout.Seconds())},
	}
	rawClientAuth, _ := proto.Marshal(clientAuth)
	packet = &protocol.Packet{
		Type: protocol.PacketType_CLIENTAUTHENTICATION,
		Body: rawClientAuth,
	}
	if err := c.writePacket(packet); err != nil {
		return err
	}

	packet.Reset()
	if err := c.readPacket(packet); err != nil {
		return err
	}
	if packet.GetType() != protocol.PacketType_ACK {
		return ErrUnexpectedPacket
	}
	var ack protocol.Ack
	if err := proto.Unmarshal(packet.GetBody(), &ack); err != nil {
		return err
	}
	if ack.GetErrorCode() > 0 {
		return errors.New("something goes wrong when doing authentication: " + ack.GetErrorMessage())
	}
	return nil
}

func (c *Client) Subscribe(filter string) error {
	packet := &protocol.Packet{}

	subscribe := &protocol.Sub{
		Destination: c.clientIdentity.destination,
		ClientId:    strconv.Itoa(c.clientIdentity.clientId),
		Filter:      filter,
	}
	packet.Type = protocol.PacketType_SUBSCRIPTION
	packet.Body, _ = proto.Marshal(subscribe)

	if err := c.writePacket(packet); err != nil {
		return err
	}

	if err := c.readPacket(packet); err != nil {
		return err
	}
	if packet.GetType() != protocol.PacketType_ACK {
		return ErrUnexpectedPacket
	}
	var ack protocol.Ack
	if err := proto.Unmarshal(packet.GetBody(), &ack); err != nil {
		return err
	}
	if ack.GetErrorCode() > 0 {
		return errors.New("failed to subscribe with reason: " + ack.GetErrorMessage())
	}
	c.clientIdentity.filter = filter
	return nil
}

func (c *Client) UnSubscribe(filter string) error {
	packet := &protocol.Packet{}
	subscribe := &protocol.Unsub{
		Destination: c.clientIdentity.destination,
		ClientId:    strconv.Itoa(c.clientIdentity.clientId),
		Filter:      filter,
	}
	packet.Type = protocol.PacketType_UNSUBSCRIPTION
	packet.Body, _ = proto.Marshal(subscribe)

	if err := c.writePacket(packet); err != nil {
		return err
	}

	if err := c.readPacket(packet); err != nil {
		return err
	}

	if packet.GetType() != protocol.PacketType_ACK {
		return ErrUnexpectedPacket
	}
	var ack protocol.Ack
	if err := proto.Unmarshal(packet.GetBody(), &ack); err != nil {
		return err
	}
	if ack.GetErrorCode() > 0 {
		return errors.New("failed to unSubscribe with reason: " + ack.GetErrorMessage())
	}
	return nil
}

func (c *Client) Ack(batchID int64) error {
	packet := &protocol.Packet{}
	clientAck := &protocol.ClientAck{
		Destination: c.clientIdentity.destination,
		ClientId:    strconv.Itoa(c.clientIdentity.clientId),
		BatchId:     batchID,
	}
	packet.Type = protocol.PacketType_CLIENTACK
	packet.Body, _ = proto.Marshal(clientAck)

	if err := c.writePacket(packet); err != nil {
		return err
	}
	return nil
}

func (c *Client) Rollback(batchID int64) error {
	packet := &protocol.Packet{}
	clientRollback := &protocol.ClientRollback{
		Destination: c.clientIdentity.destination,
		ClientId:    strconv.Itoa(c.clientIdentity.clientId),
		BatchId:     batchID,
	}
	packet.Type = protocol.PacketType_CLIENTROLLBACK
	packet.Body, _ = proto.Marshal(clientRollback)

	if err := c.writePacket(packet); err != nil {
		return err
	}
	return nil
}

func (c *Client) Get(batchSize int, timeout time.Duration) (*protocol.Message, error) {
	message, err := c.GetWithOutAck(batchSize, timeout)
	if err != nil {
		return nil, err
	}
	if err := c.Ack(message.ID); err != nil {
		return nil, err
	}
	return message, nil
}

func (c *Client) GetWithOutAck(batchSize int, timeout time.Duration) (*protocol.Message, error) {
	packet := &protocol.Packet{}

	get := &protocol.Get{
		AutoAckPresent: &protocol.Get_AutoAck{AutoAck: false},
		Destination:    c.clientIdentity.destination,
		ClientId:       strconv.Itoa(c.clientIdentity.clientId),
		FetchSize:      int32(batchSize),
		TimeoutPresent: &protocol.Get_Timeout{Timeout: int64(timeout)},
		UnitPresent:    &protocol.Get_Unit{Unit: 0},
	}
	packet.Type = protocol.PacketType_GET
	packet.Body, _ = proto.Marshal(get)

	if err := c.writePacket(packet); err != nil {
		return nil, err
	}

	if err := c.readPacket(packet); err != nil {
		return nil, err
	}

	message, err := packet.ParseMessage(c.opts.lazyParseEntry)
	if err != nil {
		return nil, err
	}

	return message, nil
}

func (c *Client) readPacket(p *protocol.Packet) error {
	if err := p.Read(c.netConn); err != nil {
		return err
	}
	return nil
}

func (c *Client) writePacket(p *protocol.Packet) error {
	if err := p.Write(c.netConn); err != nil {
		return err
	}
	return nil
}
