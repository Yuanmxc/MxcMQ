package MxcMQClient

import (
	pb "MxcMQ-Client/proto"
	"MxcMQ-Client/queue"
	"errors"

	"fmt"
)

type PartPublisher struct {
	fullName           string
	Opt                PublisherOpt
	clients            map[string]*Client
	asyncSends         map[string]*AsyncSend
	partition2fullname map[int]string
}

type AsyncSend struct {
	AsyncSendQueue *queue.Queue
	asyncSendCh    chan bool
}

func (p *PartPublisher) msg2part(key int64) int {
	part := 0
	part %= int(p.Opt.partitionNum)
	return part
}

func NewPartPulisher(srvUrl string, host string, port int, name string, topic string, partition int, opt ...PubOption) *PartPublisher {
	Option := default_publisher
	Option.srvUrl = srvUrl
	Option.host = host
	Option.port = port
	Option.name = name
	Option.topic = topic
	Option.partitionNum = int32(partition)

	for _, o := range opt {
		o.set(&Option)
	}

	p := &PartPublisher{
		Opt:                Option,
		clients:            make(map[string]*Client),
		asyncSends:         make(map[string]*AsyncSend),
		partition2fullname: make(map[int]string),
	}
	return p
}

func (p *PartPublisher) Connect() error {
	cliUrl := fmt.Sprintf("%v:%v", p.Opt.host, p.Opt.port)
	p.clients[cliUrl] = &Client{}
	if err := p.clients[cliUrl].Listen(cliUrl); err != nil {
		return err
	}

	for i := 1; i <= int(p.Opt.partitionNum); i++ {
		client := &Client{}
		name, err := client.Connect(p.Opt.srvUrl, cliUrl, p.Opt.name, p.Opt.topic, int32(i), p.Opt.ConnectTimeout)
		if err != nil {
			if derr := p.disconnect(); derr != nil {
				return err
			}
			return err
		}
		p.clients[name] = client
		p.asyncSends[name].AsyncSendQueue = queue.New()
		p.asyncSends[name].asyncSendCh = make(chan bool, p.Opt.AsyncMaxSendBufSize)
		p.partition2fullname[i] = name
	}

	for _, v := range p.asyncSends {
		go p.asyncPush(v)
	}

	return nil
}

func (p *PartPublisher) Publish(m *Msg) error {
	args := &pb.PublishArgs{
		Topic:   m.Topic,
		Mid:     nrand(),
		Payload: string(m.Data),
		Redo:    0,
	}

	switch m.Partition {
	case -1:
		args.Partition = int32(p.msg2part(args.Mid))
		// todo: update mid... to msg ?
	default:
		if m.Partition > int(p.Opt.partitionNum) || m.Partition < 0 {
			return errors.New(fmt.Sprintf("topic/partition %v does not exist", m.Partition))
		}
		args.Partition = int32(m.Partition)
	}

	for k, v := range p.clients {
		if v.Partition == args.Partition {
			args.Name = k
		}
	}
	if args.Name == "" {
		return errors.New(fmt.Sprintf("connection with topic/partition %v does not exist", args.Partition))
	}
	_, err := p.clients[args.Name].Push2serverWithRedo(args, p.Opt.OperationTimeout)
	if err != nil {
		return err
	}

	return nil
}

func (p *PartPublisher) disconnect() error {
	for _, v := range p.clients {
		if err := v.DisConnect(); err != nil {
			return err
		}
	}
	return nil
}

// callback ?
func (p *PartPublisher) AsyncPublish(m *Msg) error {
	if m.Partition > int(p.Opt.partitionNum) || m.Partition < -2 || m.Partition == 0 {
		return errors.New(fmt.Sprintf("topic/partition %v does not exist", m.Partition))
	}

	name := p.partition2fullname[m.Partition]
	if p.asyncSends[name].AsyncSendQueue.Size() >= p.Opt.AsyncMaxSendBufSize {
		return errors.New("AsyncMaxSendBufSize is full")
	}
	p.asyncSends[name].AsyncSendQueue.Push(m)
	p.asyncSends[name].asyncSendCh <- true
	return nil
}

func (p *PartPublisher) push(*Client) {

}

func (p *PartPublisher) asyncPush(as *AsyncSend) {
	for {
		<-as.asyncSendCh
		for !as.AsyncSendQueue.Empty() {
			m := as.AsyncSendQueue.Front()
			if err := p.Publish(m.(*Msg)); err != nil {
				//todo
			}
			as.AsyncSendQueue.Pop()
		}
	}
}
