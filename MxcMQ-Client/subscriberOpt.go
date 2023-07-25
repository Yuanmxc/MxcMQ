package MxcMQClient

type SubscriberOpt struct {
	name                string
	host                string
	port                int
	srvUrl              string
	ConnectTimeout      int
	OperationTimeout    int
	OperationMaxRedoNum int
}

var default_subscriber = SubscriberOpt{
	ConnectTimeout:      3,
	OperationTimeout:    3,
	OperationMaxRedoNum: 3,
}

type SuberOption interface {
	set(opt *SubscriberOpt)
}

type funcSuberOption struct {
	f func(opt *SubscriberOpt)
}

func newfuncSubscriberOption(f func(opt *SubscriberOpt)) *funcSuberOption {
	return &funcSuberOption{
		f: f,
	}
}

func (fso *funcSuberOption) set(so *SubscriberOpt) {
	fso.f(so)
}

func WithsConnectTimeout(timeout int) SuberOption {
	return newfuncSubscriberOption(func(opt *SubscriberOpt) {
		opt.ConnectTimeout = timeout
	})
}

func WithsOperationTimeout(timeout int) SuberOption {
	return newfuncSubscriberOption(func(opt *SubscriberOpt) {
		opt.OperationTimeout = timeout
	})
}

func WithsOperationMaxRedoNum(num int) SuberOption {
	return newfuncSubscriberOption(func(opt *SubscriberOpt) {
		opt.OperationMaxRedoNum = num
	})
}
