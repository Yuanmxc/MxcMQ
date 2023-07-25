package MxcMQClient

import (
	pb "MxcMQ-Client/proto"
	"MxcMQ-Client/queue"
	"errors"
	"fmt"
)

type Publisher struct {
	id        int64
	fullName  string
	Opt       PublisherOpt
	client    *Client
	asyncSend *AsyncSend
}

func NewPublisher(srvUrl string, host string, port int, name string, topic string, opt ...PubOption) (*Publisher, error) {
	Option := default_publisher
	Option.srvUrl = srvUrl
	Option.host = host
	Option.port = port
	Option.name = name
	Option.topic = topic

	for _, o := range opt {
		o.set(&Option)
	}
	if Option.partitionNum != 1 {
		return nil, errors.New("partitionNum out of range")
	}

	as := &AsyncSend{
		AsyncSendQueue: queue.New(),
		asyncSendCh:    make(chan bool, Option.AsyncMaxSendBufSize),
	}
	p := &Publisher{
		id:        nrand(),
		Opt:       Option,
		asyncSend: as,
		client:    new(Client),
	}
	p.client.OperationMaxRedoNum = int32(Option.OperationMaxRedoNum)
	return p, nil
}

func (p *Publisher) Connect() error {
	cliUrl := fmt.Sprintf("%v:%v", p.Opt.host, p.Opt.port)
	if err := p.client.Listen(cliUrl); err != nil {
		return nil
	}

	args := &pb.ConnectArgs{
		Name:         p.Opt.name,
		Url:          cliUrl,
		Redo:         0,
		Topic:        p.Opt.topic,
		Partition:    int32(p.Opt.partitionNum),
		Type:         Puber,
		PartitionNum: int32(p.Opt.partitionNum),
		PubMode:      int32(p.Opt.mode),
		Timeout:      int32(p.Opt.ConnectTimeout),
	}
	name, err := p.client.Connect(p.Opt.srvUrl, args)
	if err != nil {
		return err
	}
	p.fullName = name

	go p.asyncPush()

	return nil
}

func (p *Publisher) Publish(m *Msg) error {
	args := &pb.PublishArgs{
		Name:      p.fullName,
		Topic:     m.Topic,
		Partition: int32(m.Partition),
		Mid:       nrand(),
		Payload:   string(m.Data),
		Redo:      0,
	}
	_, err := p.client.Push2serverWithRedo(args, p.Opt.OperationTimeout)
	if err != nil {
		return err
	}
	return nil
}

// callback ?
func (p *Publisher) AsyncPublish(m *Msg) error {
	if p.asyncSend.AsyncSendQueue.Size() >= p.Opt.AsyncMaxSendBufSize {
		return errors.New("AsyncMaxSendBufSize is full")
	}
	p.asyncSend.AsyncSendQueue.Push(m)
	p.asyncSend.asyncSendCh <- true
	return nil
}

func (p *Publisher) asyncPush() {
	for {
		<-p.asyncSend.asyncSendCh
		for !p.asyncSend.AsyncSendQueue.Empty() {
			m := p.asyncSend.AsyncSendQueue.Front()
			if err := p.Publish(m.(*Msg)); err != nil {
				// todo
			}
			p.asyncSend.AsyncSendQueue.Pop()
		}
	}
}
