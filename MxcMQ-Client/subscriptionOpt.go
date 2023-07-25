package MxcMQClient

import (
	"MxcMQ-Client/queue"
	"context"
)

type subcription struct {
	// mcb                MsgHandler
	// mch                chan *Msg
	Opt                SubscriptionOpt
	clients            map[string]*Client
	receiveQueues      map[string]*ReceiveQueue
	partition2fullname map[int]string
	revQueue           *queue.Queue
	cancel             context.CancelFunc
}

type SubscriptionOpt struct {
	name             string
	mode             SubscribeMode
	topic            Topic
	partitions       []int
	subOffset        uint64
	receiveQueueSize int
	pullTimeout      int
}

type ReceiveQueue struct {
	revQueue *queue.Queue
	revCh    chan bool
}

var default_subscription = SubscriptionOpt{
	mode:             SMode_Exclusive,
	partitions:       []int{},
	subOffset:        0,
	receiveQueueSize: 100,
	pullTimeout:      25,
}

type SubscipOption interface {
	set(opt *SubscriptionOpt)
}

type funcSubscripOption struct {
	f func(opt *SubscriptionOpt)
}

func newfuncSubscripOption(f func(opt *SubscriptionOpt)) *funcSubscripOption {
	return &funcSubscripOption{
		f: f,
	}
}

func (fspo *funcSubscripOption) set(spo *SubscriptionOpt) {
	fspo.f(spo)
}

func WithspMode(mode SubscribeMode) SubscipOption {
	return newfuncSubscripOption(func(opt *SubscriptionOpt) {
		opt.mode = mode
	})
}

func WithspPartition(partitions []int) SubscipOption {
	return newfuncSubscripOption(func(opt *SubscriptionOpt) {
		opt.partitions = partitions
	})
}

func WithspSubOffset(offset int) SubscipOption {
	return newfuncSubscripOption(func(opt *SubscriptionOpt) {
		opt.subOffset = uint64(offset)
	})
}

func WithspReceiveQueueSize(size int) SubscipOption {
	return newfuncSubscripOption(func(opt *SubscriptionOpt) {
		opt.receiveQueueSize = size
	})
}

func WithspPullTimeout(timeout int) SubscipOption {
	return newfuncSubscripOption(func(opt *SubscriptionOpt) {
		opt.pullTimeout = timeout
	})
}
