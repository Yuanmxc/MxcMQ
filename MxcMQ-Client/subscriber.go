package MxcMQClient

import (
	pb "MxcMQ-Client/proto"
	"MxcMQ-Client/queue"
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type MsgHandler func(msg *Msg)

type Subscriber struct {
	id   int64
	name string
	Opt  SubscriberOpt
	sl   map[string]*subcription
}

type Topic struct {
	name         string
	partitionNum int
}

func NewSubscriber(srvUrl string, host string, port int, name string, opt ...SuberOption) *Subscriber {
	Option := default_subscriber
	Option.srvUrl = srvUrl
	Option.host = host
	Option.port = port
	Option.name = name

	for _, o := range opt {
		o.set(&Option)
	}

	s := &Subscriber{
		id:   nrand(),
		name: name,
		Opt:  Option,
		sl:   make(map[string]*subcription),
	}

	return s
}

func (s *Subscriber) getTopic(sub *subcription) (*Topic, error) {
	client := &Client{
		OperationMaxRedoNum: int32(s.Opt.OperationMaxRedoNum),
	}
	conn, err := grpc.Dial(s.Opt.srvUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	args := &pb.GetTopicInfoArgs{
		Topic: sub.Opt.topic.name,
		Redo:  0,
	}
	client.conn = conn
	reply, err := client.GetTopicWithRedo(args, s.Opt.OperationTimeout)
	if err != nil {
		return nil, err
	}
	topic := &Topic{
		partitionNum: int(reply.PartitionNum),
	}
	return topic, nil
}

func (s *Subscriber) connect(sub *subcription, i int) error {
	cliUrl := fmt.Sprintf("%v:%v", s.Opt.host, s.Opt.port)
	// for i := 1; i <= sub.Opt.topic.partitionNum; i++ {
	client := &Client{OperationMaxRedoNum: int32(s.Opt.OperationMaxRedoNum)}
	args := &pb.ConnectArgs{
		Name:         s.Opt.name,
		Url:          cliUrl,
		Redo:         0,
		Topic:        sub.Opt.topic.name,
		Partition:    int32(i),
		Timeout:      int32(s.Opt.ConnectTimeout),
		PartitionNum: int32(i),
	}
	//record
	name, err := client.Connect(s.Opt.srvUrl, args)
	if err != nil {
		// disconnect exist
		return err
	}
	sub.clients[name] = client
	sub.receiveQueues[name] = &ReceiveQueue{
		revQueue: queue.New(),
		revCh:    make(chan bool, sub.Opt.receiveQueueSize),
	}
	sub.partition2fullname[i] = name
	// }

	return nil
}

func (s *Subscriber) Subscribe(name string, topic string, opt ...SubscipOption) (*subcription, error) {
	Opt := default_subscription
	Opt.name = name
	Opt.topic.name = topic

	for _, o := range opt {
		o.set(&Opt)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sub := &subcription{
		Opt:                Opt,
		clients:            make(map[string]*Client),
		receiveQueues:      make(map[string]*ReceiveQueue),
		partition2fullname: make(map[int]string),
		revQueue:           queue.New(),
		cancel:             cancel,
	}

	cliUrl := fmt.Sprintf("%v:%v", s.Opt.host, s.Opt.port)
	if err := sub.clients[cliUrl].Listen(cliUrl); err != nil {
		return nil, err
	}

	s.sl[sub.Opt.name] = sub

	t, err := s.getTopic(sub)
	if err != nil {
		return nil, err
	}
	sub.Opt.topic.partitionNum = t.partitionNum

	// if err := s.Connect(sub); err != nil {
	// 	return err
	// }

	switch len(sub.Opt.partitions) {
	case 0:
		for i := 1; i <= sub.Opt.topic.partitionNum; i++ {
			if err := s.connect(sub, i); err != nil {
				return nil, err
			}

			name := sub.partition2fullname[i]
			args := &pb.SubscribeArgs{
				Name:         name,
				Topic:        sub.Opt.topic.name,
				Partition:    int32(i),
				SubOffset:    sub.Opt.subOffset,
				Subscription: sub.Opt.name,
				Mode:         pb.SubscribeArgs_SubMode(sub.Opt.mode),
				Redo:         0,
			}
			_, err := sub.clients[name].Subscribe(args, s.Opt.OperationTimeout)
			if err != nil {
				if sub.clients[name].GetErrorString(err) == "Repeat subscription" {
					fmt.Println("Repeat subscription")
				} else {
					return nil, err
				}
			}

			go sub.pull(ctx, i)
			go sub.receive(ctx, i)
		}
	default:
		for _, partition := range sub.Opt.partitions {
			if partition > sub.Opt.topic.partitionNum || partition <= 0 {
				//unsubscribe exist
				return nil, errors.New(fmt.Sprintf("topic/partition %v does not exist", partition))
			}

			if err := s.connect(sub, partition); err != nil {
				return nil, err
			}

			name := sub.partition2fullname[partition]
			args := &pb.SubscribeArgs{
				Name:         name,
				Topic:        sub.Opt.topic.name,
				Partition:    int32(partition),
				SubOffset:    sub.Opt.subOffset,
				Subscription: sub.Opt.name,
				Mode:         pb.SubscribeArgs_SubMode(sub.Opt.mode),
				Redo:         0,
			}
			_, err := sub.clients[name].SubscribeWithRedo(args, s.Opt.OperationTimeout)
			if err != nil {
				return nil, err
			}
			go sub.pull(ctx, partition)
			go sub.receive(ctx, partition)
		}
	}
	return sub, nil
}

func (sub *subcription) pull(ctx context.Context, partition int) {
	name := sub.partition2fullname[partition]
	for {
		bufSize := sub.Opt.receiveQueueSize - sub.receiveQueues[name].revQueue.Size()
		//todo: consider 90% ?
		args := &pb.PullArgs{
			Name:         name,
			Topic:        sub.Opt.topic.name,
			Partition:    int32(partition),
			Subscription: sub.Opt.name,
			BufSize:      int32(bufSize),
			Timeout:      int32(sub.Opt.pullTimeout),
			Redo:         0,
		}
		fmt.Println("send pull to server")
		_, err := sub.clients[name].Pull(args, sub.Opt.pullTimeout)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (sub *subcription) receive(ctx context.Context, partition int) {
	for {
		name := sub.partition2fullname[partition]
		msg := <-sub.clients[name].msgCh
		sub.revQueue.Push(msg)
	}
}

func (sub *subcription) Receive() (*Msg, error) {
	msg := sub.revQueue.Pop()
	if msg == nil {
		return nil, errors.New("no more message")
	}
	return msg.(*Msg), nil
}

func (s *Subscriber) Unsubscribe(sub *subcription) error {
	if _, ok := s.sl[sub.Opt.name]; !ok {
		return errors.New("subscription does not exist")
	}

	switch len(sub.Opt.partitions) {
	case 0:
		for i := 1; i < sub.Opt.topic.partitionNum; i++ {
			name := sub.partition2fullname[i]
			args := &pb.UnSubscribeArgs{
				Name:         name,
				Topic:        sub.Opt.topic.name,
				Partition:    int32(i),
				Subscription: sub.Opt.name,
				Redo:         0,
			}
			_, err := sub.clients[name].UnSubscribeWithRedo(args, s.Opt.OperationTimeout)
			if err != nil {
				return err
			}
		}
	default:
		for _, partition := range sub.Opt.partitions {
			name := sub.partition2fullname[partition]
			args := &pb.UnSubscribeArgs{
				Name:         name,
				Topic:        sub.Opt.topic.name,
				Partition:    int32(partition),
				Subscription: sub.Opt.name,
				Redo:         0,
			}
			_, err := sub.clients[name].UnSubscribeWithRedo(args, s.Opt.OperationTimeout)
			if err != nil {
				return err
			}
		}
	}

	delete(s.sl, sub.Opt.name)
	sub.cancel()
	return nil
}
