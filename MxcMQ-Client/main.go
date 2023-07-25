package MxcMQClient

import (
	"bufio"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"sync"
	"time"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Client struct {
	mu      sync.Mutex
	conn    net.Conn
	url     *url.URL
	sl      map[string]*subcription
	scratch [512]byte
	err     error
	bw      *bufio.Writer
	ps      praseState
	op      *Options
	fch     chan bool

	bi  Broker
}

type Options struct {
	Url            string
	AllowReconnect bool
	MaxReconnect   int
	ReconnectWait  time.Duration
	Timeout        time.Duration
}

type Msg struct {
	id        int64
	topic     string
	data      []byte
	mode      int
	ch        chan bool
	redo      int
	partition int
}

type subcription struct {
	name      string
	topic     string
	subType   int
	mcb       MsgHandler
	mch       chan *Msg
	partition int
	clients   map[string]*Client
}

type praseState struct {
	state  int
	as     int
	drop   int
	ma     msgArg
	argBuf []byte
	msgBuf []byte
}

type srv struct {
	url         *url.URL
	didConnect  bool
	reconnects  int
	lastAttempt time.Time
}

const (
	Exclusive = iota
	Shared
	Key_Shared
)

const (
	_CRLF_  = "\r\n"
	_PUB_P_ = "PUB "
)

const (
	conProto   = "CONNECT %s" + _CRLF_
	pubProto   = "PUB %s %s %s" + _CRLF_
	subProto   = "SUB %s %s %d" + _CRLF_
	unsubProto = "UNSUB %s"
)

const (
	DefaultURL           = "nats://localhost:4222"
	DefaultPort          = 4222
	DefaultMaxReconnect  = 10
	DefaultReconnectWait = 2 * time.Second
	DefaultTimeout       = 2 * time.Second
)

var DefaultOptions = Options{
	AllowReconnect: true,
	MaxReconnect:   DefaultMaxReconnect,
	ReconnectWait:  DefaultReconnectWait,
	Timeout:        DefaultTimeout,
}

var defaultBufSize = 65536

func NewClient(op *Options) *Client {
	c := &Client{}
	c.op = op
	c.sl = make(map[string]*subcription)

	return c
}

func Connect(url string) (*Client, error) {
	opts := DefaultOptions
	opts.Url = url
	return opts.Connect()
}

func (op *Options) Connect() (*Client, error) {
	c := NewClient(op)
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) connect() error {
	c.url, c.err = url.Parse(c.op.Url)
	if c.err != nil {
		return c.err
	}
	if err := c.createConn(); err != nil {
		return err
	} else {
		if err = c.processConnectInit(); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) createConn() error {
	c.conn, c.err = net.DialTimeout("tcp", c.url.Host, c.op.Timeout)
	if c.err != nil {
		return c.err
	}
	c.bw = bufio.NewWriterSize(c.bw, defaultBufSize)
	return nil
}

func (c *Client) processConnectInit() error {
	// c.setup()

	go c.spinUpSocketWatchers()
	return nil
}

// func (c *Client) setup() {
// 	c.sl = make(map[string]*subcription)
// }

func (c *Client) spinUpSocketWatchers() {
	go c.readLoop()
	go c.flusher()
}

func (c *Client) Publish(topic string, msg []byte) error {
	m := &Msg{}
	m.ch = make(chan bool)
	m.data = msg
	m.id = nrand()

	return c.publish(m)
}

const digits = "0123456789"

func (c *Client) publish(m *Msg) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	msgh := c.scratch[:len(_PUB_P_)]
	msgh = append(msgh, []byte(m.topic)...)
	msgh = append(msgh, ' ')
	// if reply != "" {
	// 	msgh = append(msgh, []byte(reply)...)
	// 	msgh = append(msgh, ' ')
	// }

	var b [12]byte
	var i = len(b)
	if len(m.data) > 0 {
		for l := len(m.data); l > 0; l /= 10 {
			i -= 1
			b[i] = digits[l%10]
		}
	} else {
		i -= 1
		b[i] = digits[0]
	}
	msgh = append(msgh, b[i:]...)

	msgh = append(msgh, []byte(_CRLF_)...)

	if _, c.err = c.bw.Write(msgh); c.err != nil {
		return c.err
	}
	if _, c.err = c.bw.Write(m.data); c.err != nil {
		return c.err
	}

	//TODO: wait resp
	// c.deliverMsg()

	return nil
}

type MsgHandler func(msg *Msg)

func (c *Client) subscribe(sub *subcription) error {
	return nil
}

func (c *Client) subsribe(topic string, sub string, subType int, cb MsgHandler) (*subcription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s := &subcription{name: sub, topic: topic, subType: subType}
	if cb != nil {
		go c.deliverMsg(s)
	}
	c.sl[sub] = s

	c.bw.WriteString(fmt.Sprintf(subProto, topic, sub, subType))

	return s, nil
}

func (c *Client) readLoop() {
	buf := make([]byte, defaultBufSize)
	for {
		n, err := c.conn.Read(buf)
		if err != nil {
			c.processOpErr(err)
			break
		}
		if err = c.prase(buf[:n]); err != nil {
			c.processOpErr(err)
			break
		}
	}
}

func (c *Client) flusher() {
	for {
		_, ok := <-c.fch
		if ok {
			return
		}
		if b := c.bw.Buffered(); b > 0 {
			c.err = c.bw.Flush()
		}
	}
}

func (c *Client) processOpErr(err error) {

}

func (c *Client) deliverMsg(sub *subcription) {
	for {
		msg, ok := <-sub.mch
		if !ok {
			continue
		}
		sub.mcb(msg)
	}
}

func (c *Client) processMsg(msg []byte) {
	sub := c.sl[c.ps.ma.sub]
	if sub == nil {
		return
	}

	topic := c.ps.ma.topic
	m := &Msg{topic: topic, data: msg}

	if sub.mch != nil {
		//TODO: slow consumer
		sub.mch <- m
	}
}

const argsLenMax = 4

func (c *Client) processMsgArg(arg []byte) error {
	args := spiltArg(arg)
	switch len(args) {
	case 3:
		c.ps.ma.topic = string(args[0])
		c.ps.ma.sub = string(args[1])
		//TODO
		c.ps.ma.size = int(parseInt64(args[2]))
	case 4:
		c.ps.ma.topic = string(args[0])
		c.ps.ma.sub = string(args[1])
		//TODO
		c.ps.ma.size = int(parseInt64(args[3]))
	default:
		return errors.New("")
	}
	if c.ps.ma.size < 0 {
		return errors.New("")
	}
	return nil
}

func spiltArg(arg []byte) [][]byte {
	a := [argsLenMax][]byte{}
	args := a[:0]
	start := -1
	for i, en := range arg {
		switch en {
		case ' ', '\n', '\t', '\r':
			if start >= 0 {
				args = append(args, arg[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		args = append(args, arg[start:])
	}
	return args
}

const (
	ascii_0 = 48
	ascii_9 = 57
)

func parseInt64(b []byte) (n int64) {
	if len(b) == 0 {
		return -1
	}
	for _, dec := range b {
		if dec < ascii_0 || dec > ascii_9 {
			return -1
		}
		n = n*10 + (int64(dec) - ascii_0)
	}
	return n
}
