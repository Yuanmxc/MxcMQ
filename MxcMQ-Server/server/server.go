package server

import (
	"MxcMQ-Server/bundle"
	ct "MxcMQ-Server/collect"
	"MxcMQ-Server/config"
	"MxcMQ-Server/logger"
	"MxcMQ-Server/msg"
	"MxcMQ-Server/persist"
	rc "MxcMQ-Server/registrationCenter"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	lm "MxcMQ-Server/loadManager"
	pb "MxcMQ-Server/proto"

	"github.com/samuel/go-zookeeper/zk"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Server struct {
	Info       *rc.BrokerNode
	Running    bool
	Sl         *sublist
	ps         map[string]*partitionData
	partitions sync.Map

	gcid    uint64 // deprecate
	kv      clientv3.KV
	bundles *bundle.Bundles

	grpcServer *grpc.Server
	conns      sync.Map

	// bundle2broker map[bundle.BundleInfo]rc.BrokerNode

	loadManager *lm.LoadManager

	pb.UnimplementedServerServer
}

type partitionData struct {
	mu     sync.Mutex
	pNode  *rc.PartitionNode
	msgs   sync.Map
	pubers []int64
}

const (
	subcriptionKey = "/%s/p%d/%s" // topic/partition/subcriptionName
	msgKey         = "/%s/p%d/%d" // topic/partition/msid
	partitionKey   = "/%s/p%d"    //topic/partition
)

// subscribe/publish mode
const (
	Exclusive     = iota
	WaitExclusive //pub only
	Failover      // sub only
	Shared
	Key_Shared // sub only
)

type PublishMode int32

type SubscribeMode int32

type RouteMode int32

const (
	SMode_Exclusive SubscribeMode = 0
	SMode_Failover  SubscribeMode = 1
	SMode_Shard     SubscribeMode = 2
	SMode_KeyShard  SubscribeMode = 3
)

const (
	RMode_RoundRobinPartition RouteMode = 0
	RMode_CustomPartition     RouteMode = 1
)

const (
	PMode_Exclusive     PublishMode = 0
	PMode_WaitExclusive PublishMode = 1
	PMode_Shared        PublishMode = 2
)

const (
	Puber = iota
	PartPuber
	Suber
)

var defaultSendSize int

func NewServerFromConfig() *Server {
	s := &Server{
		ps: make(map[string]*partitionData),
		kv: clientv3.NewKV(persist.EtcdCli),
		Sl: NewSublist(),
		// bundle2broker: make(map[bundle.BundleInfo]rc.BrokerNode),
	}
	s.Info = &rc.BrokerNode{
		Name:      config.SrvConf.Name,
		Host:      config.SrvConf.Host,
		Port:      config.SrvConf.Port,
		Pnum:      0,
		LoadIndex: 0,
	}

	s.Info.Load.Cpu.Limit = config.SrvConf.CpuLimit
	s.Info.Load.VirtualMemory.Limit = config.SrvConf.VirtualMemoryLimit
	s.Info.Load.SwapMemory.Limit = config.SrvConf.SwapMemoryLimit
	s.Info.Load.BandwidthIn.Limit = config.SrvConf.BandwidthInLimit
	s.Info.Load.BandwidthOut.Limit = config.SrvConf.BandwidthOutLimit

	s.grpcServer = grpc.NewServer()

	s.loadManager = lm.NewLoadManager(s.Info)
	return s
}

func (s *Server) Online() (err error) {
	isExists, err := rc.ZkCli.IsBrokerExists(s.Info.Name)
	if err != nil {
		return err
	}
	if isExists {
		return logger.Errorf("there is a exist broker %v", s.Info.Name)
	}

	s.Info.Load, err = ct.CollectLoadData()
	if err != nil {
		return logger.Errorf("CollectLoadData failed: %v", err)
	}
	if err := rc.ZkCli.RegisterBnode(*s.Info); err != nil {
		return err
	}

	s.bundles, err = bundle.NewBundles()
	if err != nil {
		panic(logger.Errorf("NewBundles failed: %v", err))
	}

	s.loadManager.Run()
	return nil
}

func (s *Server) RunWithGrpc() {
	address := fmt.Sprintf("%v:%v", s.Info.Host, s.Info.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(logger.Errorf("failed to listen: %v", err))
	}
	logger.Infof("server listening at %v", listener.Addr())

	pb.RegisterServerServer(s.grpcServer, s)
	if err := s.grpcServer.Serve(listener); err != nil {
		logger.Warnf("failed to serve: %v", err)
	}
}

func (s *Server) ShutDown() {
	s.grpcServer.GracefulStop()
	// notify registry
}

func (s *Server) Run() {
	s.Running = true
	s.AcceptLoop()
}

func (s *Server) isrunning() bool {
	isrunning := s.Running
	return isrunning
}

func (s *Server) AcceptLoop() {
	listener, err := net.Listen("tcp", "0.0.0.0:4222")
	if err != nil {
		logger.Debugf("net.Listen failed %v", err)
	}
	logger.Debugf("Listening on %v", listener.Addr())
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Errorf("accept failed: %v", err)
		}
		s.createClient(conn)
	}
}

func (s *Server) createClient(conn net.Conn) *client {
	c := &client{conn: conn, srv: s}
	c.cid = atomic.AddUint64(&s.gcid, 1)
	c.bw = bufio.NewWriterSize(c.conn, defaultBufSize)
	c.br = bufio.NewReaderSize(c.conn, defaultBufSize)

	go c.readLoop()
	return c
}

// func (s *Server) put(m msg.PubArg) error {
// 	key := fmt.Sprintf("%s/%d/%v", m.Topic, m.Partition, m.Msid)
// 	val := fmt.Sprintf("%d/r/n%v", m.Mid, m.Payload)
// 	_, err := s.kv.Put(context.TODO(), key, val)

// 	return err
// }
func (s *Server) PutMsg(m *msg.PubArg, mData msg.MsgData) (string, error) {
	key := fmt.Sprintf(msgKey, m.Topic, m.Partition, mData.Msid)
	data, err := json.Marshal(mData)
	if err != nil {
		return "", err
	}
	return key, s.put(key, data)
}

func (s *Server) PutSubcription(sub *subcription) error {
	key := fmt.Sprintf(subcriptionKey, sub.Data.Meta.TopicName, sub.Data.Meta.Partition, sub.Data.Meta.Name)
	logger.Debugf("PutSubcription: %v-%v", key, sub)
	data, err := json.Marshal(sub.Data)
	if err != nil {
		return err
	}
	return s.put(key, data)
}

func (s *Server) put(key string, data []byte) error {
	_, err := s.kv.Put(context.TODO(), key, string(data))
	return err
}

func (s *Server) GetSubcription(sNode *rc.SubcriptionNode) (*subcription, error) {
	sub := NewSubcription()
	key := fmt.Sprintf(subcriptionKey, sNode.TopicName, sNode.Partition, sNode.Name)
	data, err := s.get(key)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(data, sub.Data); err != nil {
		return nil, err
	}
	//TODO: connect between broker and suber ?
	return sub, nil
}

func (s *Server) GetMsg(pua *msg.PullArg, msid uint64) (*msg.MsgData, error) {
	m := &msg.MsgData{}
	key := fmt.Sprintf(msgKey, pua.Topic, pua.Partition, msid)
	data, err := s.get(key)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Server) get(key string) ([]byte, error) {
	resp, err := s.kv.Get(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	//TODO: resp.Count
	if len(resp.Kvs) <= 0 {
		return nil, errors.New(fmt.Sprintf("key: %v not has value", key))
	}
	return resp.Kvs[0].Value, nil
}

func (s *Server) DeleteMsg(topic string, partition int, msid uint64) error {
	key := fmt.Sprintf(msgKey, topic, partition, msid)
	return s.pDelete(key)
}

func (s *Server) pDelete(key string) error {
	_, err := s.kv.Delete(context.TODO(), key)
	return err
}

func (s *Server) Connect(ctx context.Context, args *pb.ConnectArgs) (*pb.ConnectReply, error) {
	logger.Infof("Receive Connect rq from %v", args)
	reply := &pb.ConnectReply{}
	conn, err := grpc.Dial(args.Url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return reply, err
	}

	tNode, err := rc.ZkCli.GetTopic(args.Topic)
	if err != nil {
		if err == zk.ErrNoNode {
			topicNode := &rc.TopicNode{
				Name:       args.Topic,
				Pnum:       int(args.PartitionNum),
				PulishMode: int(args.PubMode),
			}
			if err := s.registerTopic(topicNode); err != nil {
				logger.Errorf("registerTopic failed: %v", err)
				conn.Close()
				return reply, errors.New("404")
			} else {
				tNode = topicNode
			}
		} else {
			logger.Errorf("GetTopic failed: %v", err)
			conn.Close()
			return reply, errors.New("404")
		}
	}

	pNode := &rc.PartitionNode{
		ID:         int(args.Partition),
		TopicName:  args.Topic,
		Mnum:       0,
		AckOffset:  0,
		PushOffset: 0,
		Version:    0,
		// Url: ,
	}
	if err := rc.ZkCli.RegisterPnode(pNode); err != nil && err != zk.ErrNodeExists {
		logger.Errorf("RegisterPnode failed: %v", err)
		conn.Close()
		return reply, errors.New("404")
	}

	preName := fmt.Sprintf(rc.PnodePath, rc.ZkCli.ZkTopicRoot, args.Topic, args.Partition)
	switch args.Type {
	case Puber, PartPuber:
		preName = preName + "-publisher-" + args.Name

		switch PublishMode(tNode.PulishMode) {
		case PMode_Exclusive:
			isExists, err := rc.ZkCli.IsPubersExists(args.Topic, int(args.Partition))
			if err != nil {
				logger.Errorf("IsPubersExists failed: %v", err)
				conn.Close()
				return reply, errors.New("404")
			}

			if isExists {
				pubNode, err := rc.ZkCli.GetLeadPuber(args.Topic, int(args.Partition))
				if err != nil {
					logger.Errorf("GetPuber failed: %v", err)
					conn.Close()
					return reply, errors.New("404")
				}

				if pubNode.ID != args.Id {
					logger.Debugln("This Exclusive topic already has a puber")
					conn.Close()
					return reply, errors.New("This Exclusive topic already has a puber")
				}
			} else {
				if err := rc.ZkCli.RegisterLeadPuberNode(args.Topic, int(args.Partition), args.Id); err != nil {
					logger.Errorf("RegisterLeadPuberNode failed: %v", err)
					conn.Close()
					return reply, errors.New("404")
				}
				go s.ClientAlive(conn, *args)
			}
		case PMode_WaitExclusive:
			isExists, err := rc.ZkCli.IsPubersExists(args.Topic, int(args.Partition))
			if err != nil {
				logger.Errorf("IsPubersExists failed: %v", err)
				conn.Close()
				return reply, errors.New("404")
			}
			if isExists {
				ch := make(chan error)
				go handlePmode_wait(ctx, args, ch)
				select {
				case err := <-ch:
					if err != nil {
						return nil, err
					}
				case <-ctx.Done():
					return nil, status.Error(codes.Canceled, "timed out")
				}
				if err := rc.ZkCli.RegisterLeadPuberNode(args.Topic, int(args.Partition), args.Id); err != nil {
					logger.Errorf("RegisterLeadPuberNode failed: %v", err)
					conn.Close()
					return reply, errors.New("404")
				}
				go s.ClientAlive(conn, *args)
			} else {
				if err := rc.ZkCli.RegisterLeadPuberNode(args.Topic, int(args.Partition), args.Id); err != nil {
					logger.Errorf("RegisterLeadPuberNode failed: %v", err)
					conn.Close()
					return reply, errors.New("404")
				}
				go s.ClientAlive(conn, *args)
			}
		case PMode_Shared:
			if err := rc.ZkCli.RegisterPuberNode(args.Topic, int(args.Partition), args.Id); err != nil {
				if err != zk.ErrNodeExists {
					logger.Errorf("RegisterPuberNode failed: %v", err)
					conn.Close()
					return reply, errors.New("404")
				}
			}
		}
	case Suber:
		preName = preName + "-subscriber-" + args.Name
	}

	curName := preName
	if config.SrvConf.AllowRenameForClient {
		index := 1
		for {
			if _, ok := s.conns.Load(curName); ok {
				curName = preName
				curName += "(" + strconv.Itoa(index) + ")"
				index++
			} else {
				break
			}
		}
	} else {
		if _, ok := s.conns.Load(curName); ok {
			logger.Infoln("Name conflict, rename plz")
			return nil, errors.New("Name conflict, rename plz")
		}
	}

	s.conns.Store(curName, conn)
	reply.Name = curName
	if preName != curName {
		return reply, errors.New("Automatically rename")
	}
	logger.Debugf("Connect reply: %v", reply)
	return reply, nil
}

func handlePmode_wait(ctx context.Context, args *pb.ConnectArgs, over chan<- error) {
	_, ch, err := rc.ZkCli.RegisterLeadPuberWatch(args.Topic, int(args.Partition))
	if err != nil {
		over <- err
	}

	select {
	case <-ctx.Done():
		logger.Infof("wait timed out %v", args)
	case <-ch:
		over <- nil
	}
}

func handleSmode_Failover(ctx context.Context, args *pb.SubscribeArgs, over chan<- error) {
	_, ch, err := rc.ZkCli.RegisterLeadSuberWatch(args.Topic, int(args.Partition), args.Subscription)
	if err != nil {
		over <- err
	}

	select {
	case <-ctx.Done():
		logger.Infof("wait timed out %v", args)
	case <-ch:
		over <- nil
	}
}

func (s *Server) registerTopic(tNode *rc.TopicNode) error {
	if err := rc.ZkCli.RegisterTnode(tNode); err != nil {
		return err
	}

	return nil
}

func (c *client) waitcallback(ch <-chan zk.Event) {
	<-ch
}

func (s *Server) ProcessSub(ctx context.Context, args *pb.SubscribeArgs) (*pb.SubscribeReply, error) {
	logger.Infof("Receive Subscribe rq from %v", args)
	reply := &pb.SubscribeReply{}
	sub := NewSubcription()
	sub.Data.Meta.TopicName = args.Topic
	sub.Data.Meta.Partition = int(args.Partition)
	sub.Data.Meta.Name = args.Subscription
	sub.Data.Meta.Subtype = int(args.Mode)

	snode := &rc.SubcriptionNode{
		Name:      sub.Data.Meta.Name,
		TopicName: sub.Data.Meta.TopicName,
		Partition: sub.Data.Meta.Partition,
		Subtype:   sub.Data.Meta.Subtype,
	}

	var exSub *subcription
	key := fmt.Sprintf(subcriptionKey, snode.TopicName, snode.Partition, snode.Name)
	if sub, ok := s.Sl.Subs[key]; ok {
		exSub = sub
	} else {
		isExists, err := rc.ZkCli.IsSubcriptionExist(snode)
		if err != nil {
			logger.Errorf("IsSubcriptionExist failed: %v", err)
			return reply, errors.New("404")
		}
		if isExists {
			existSnode, err := rc.ZkCli.GetSub(snode)
			if err != nil {
				logger.Errorf("GetSub failed: %v", err)
				return reply, errors.New("404")
			}
			if existSnode.Subtype != snode.Subtype {
				logger.Warnln("there is confict between existing subcription and yours")
				return reply, errors.New("there is confict between existing subcription and yours")
			}
			existSdata, err := s.GetSubcription(existSnode)
			if err != nil {
				logger.Errorf("GetSubcription failed: %v", err)
				return reply, errors.New("404")
			}
			exSub = existSdata
			s.Sl.Subs[key] = exSub
		}
	}

	if exSub == nil {
		if err := rc.ZkCli.RegisterSnode(snode); err != nil {
			logger.Errorf("RegisterSnode failed: %v", err)
			return reply, errors.New("404")
		}
		// conn, _ := s.conns.LoadAndDelete(args.Name)
		// sub.Data.Subers[args.Name] = args.Name
		// sub.clients[args.Name] = conn.(*grpc.ClientConn)
		s.Sl.Subs[key] = sub
		exSub = sub
	}

	if exSub.Data.Meta.Subtype == snode.Subtype && exSub.Data.Subers[args.Name] == args.Name {
		logger.Warnln("Repeat subscription, change connection")
		// return reply, errors.New("Repeat subscription")
		conn, _ := s.conns.LoadAndDelete(args.Name)
		exSub.Data.Subers[args.Name] = args.Name
		exSub.clients[args.Name] = conn.(*grpc.ClientConn)
	} else {
		switch SubscribeMode(exSub.Data.Meta.Subtype) {
		case SMode_Exclusive:
			if len(exSub.Data.Subers) == 0 {
				conn, _ := s.conns.LoadAndDelete(args.Name)
				exSub.Data.Subers[args.Name] = args.Name
				exSub.clients[args.Name] = conn.(*grpc.ClientConn)

				if err := rc.ZkCli.RegisterLeadSuberNode(args.Topic, int(args.Partition), args.Subscription, args.Id); err != nil {
					logger.Errorf("RegisterLeadSuberNode failed: %v", err)
					// conn.Close()
					return reply, errors.New("404")
				}

				if err := s.PutSubcription(exSub); err != nil {
					logger.Errorf("PutSubcription failed: %v", err)
					return reply, errors.New("404")
				}
			} else {
				logger.Warnln("there is a suber in existing subcription")
				return reply, errors.New("there is a suber in existing subcription")
			}
		case SMode_Failover:
			isExists, err := rc.ZkCli.IsSubersExists(args.Topic, int(args.Partition), args.Subscription)
			if err != nil {
				logger.Errorf("IsSubersExists failed: %v", err)
				//todo: close conn
				return reply, errors.New("404")
			}

			if isExists {
				ch := make(chan error)
				go handleSmode_Failover(ctx, args, ch)
				select {
				case err := <-ch:
					if err != nil {
						return nil, err
					}
				case <-ctx.Done():
					return nil, status.Error(codes.Canceled, "timed out")
				}
				if err := rc.ZkCli.RegisterLeadSuberNode(args.Topic, int(args.Partition), args.Subscription, args.Id); err != nil {
					logger.Errorf("RegisterLeadSuberNode failed: %v", err)
					// conn.Close()
					return reply, errors.New("404")
				}
				conn, _ := s.conns.LoadAndDelete(args.Name)
				exSub.Data.Subers[args.Name] = args.Name
				exSub.clients[args.Name] = conn.(*grpc.ClientConn)
				go s.SuberAlive(conn.(*grpc.ClientConn), *args)
			} else {
				if err := rc.ZkCli.RegisterLeadSuberNode(args.Topic, int(args.Partition), args.Subscription, args.Id); err != nil {
					logger.Errorf("RegisterLeadSuberNode failed: %v", err)
					// conn.Close()
					return reply, errors.New("404")
				}
				conn, _ := s.conns.LoadAndDelete(args.Name)
				exSub.Data.Subers[args.Name] = args.Name
				exSub.clients[args.Name] = conn.(*grpc.ClientConn)
				go s.SuberAlive(conn.(*grpc.ClientConn), *args)
			}
		case SMode_Shard:
			conn, _ := s.conns.LoadAndDelete(args.Name)
			exSub.Data.Subers[args.Name] = args.Name
			exSub.clients[args.Name] = conn.(*grpc.ClientConn)

			if err := rc.ZkCli.RegisterSuberNode(args.Topic, int(args.Partition), args.Subscription, args.Id); err != nil {
				if err != zk.ErrNodeExists {
					logger.Errorf("RegisterSuberNode failed: %v", err)
					return reply, errors.New("404")
				}
			}
			//TODO: need some extra action
		}
	}

	name := fmt.Sprintf(partitionKey, args.Topic, args.Partition)
	if _, ok := s.partitions.Load(name); !ok {
		pNode, err := rc.ZkCli.GetPartition(args.Topic, int(args.Partition))
		if err != nil {
			logger.Errorf("GetPartition failed: %v", err)
			return nil, errors.New("404")
		}

		s.partitions.Store(name, &partitionData{
			pNode: pNode,
		})
	}

	p, _ := s.partitions.Load(name)
	p.(*partitionData).mu.Lock()
	pData := p.(*partitionData)
	switch args.SubOffset {
	case 0:
		sub.Data.PushOffset = pData.pNode.PushOffset + 1
	default:
		if sub.Data.PushOffset >= pData.pNode.Mnum {
			sub.Data.PushOffset = pData.pNode.PushOffset + 1
		} else {
			sub.Data.PushOffset = args.SubOffset
		}
	}
	p.(*partitionData).mu.Unlock()

	if err := s.PutSubcription(sub); err != nil {
		reply.Error = err.Error()
		return reply, err
	}
	logger.Debugf("handle subscribe over: %v", reply)
	return reply, nil
}

func (s *Server) ProcessPull(ctx context.Context, args *pb.PullArgs) (*pb.PullReply, error) {
	logger.Infof("Receive Pull rq from %v", args)
	reply := &pb.PullReply{}
	pua := &msg.PullArg{
		Topic:     args.Topic,
		Partition: int(args.Partition),
		Subname:   args.Subscription,
		Bufsize:   int(args.BufSize),
		Full:      make(chan bool),
		Timeout:   make(chan bool),
	}

	pkey := fmt.Sprintf(partitionKey, pua.Topic, pua.Partition)
	p, _ := s.partitions.Load(pkey)
	pNode := p.(*partitionData)
	logger.Debugln(pkey, " ", pNode)
	skey := fmt.Sprintf(subcriptionKey, pua.Topic, pua.Partition, pua.Subname)
	exSub := s.Sl.Subs[skey]

	go pua.CheckTimeout(int(args.Timeout))

	for {
		select {
		case <-pua.Timeout:
			logger.Infof("Pull rq timed out %v", args)
			reply.Error = "time out"
			return reply, nil
		default:
			// pNode.mu.Lock()
			if pua.Bufsize <= 0 {
				pua.Full <- true
				reply.Error = "buffer is full"
				return reply, nil
			}

			exSub.mu.Lock()
			i := exSub.Data.PushOffset
			if i <= pNode.pNode.Mnum {
				var m *msg.MsgData
				key := fmt.Sprintf(msgKey, pua.Topic, pua.Partition, i)
				if en, ok := pNode.msgs.Load(key); ok {
					m = en.(*msg.MsgData)
				} else {
					ms, err := s.GetMsg(pua, i)
					if err != nil {
						reply.Error = err.Error()
						return reply, err
					}
					m = ms
				}
				i++
				exSub.mu.Unlock()
				mArgs := &pb.MsgArgs{
					Name:      key,
					Topic:     args.Topic,
					Partition: args.Partition,
					Mid:       m.Mid,
					Msid:      m.Msid,
					Payload:   m.Payload,
					Redo:      0,
					Suber:     args.Name,
				}
				_, err := s.sendMsg(mArgs, exSub, config.SrvConf.OperationTimeout)
				if err != nil {
					logger.Errorf("sendMsgWithRedo failed: %v", err)
					// dead letter
				}
				pua.Bufsize--

				pNode.mu.Lock()
				if pNode.pNode.PushOffset < i {
					pNode.pNode.PushOffset = i
					if err := rc.ZkCli.UpdatePartition(pNode.pNode); err != nil {
						logger.Errorf("UpdatePartition failed: %v", err)
					}
				}
				pNode.mu.Unlock()
				// switch exSub.Data.Meta.Subtype{
				// 	case
				// }
				if exSub.Data.PushOffset < i {
					exSub.Data.PushOffset = i
					if err := s.PutSubcription(exSub); err != nil {
						logger.Errorf("PutSubcription failed: %v", err)
					}
				}
			} else {
				exSub.mu.Unlock()
			}
		}
	}
	// return reply, nil
}

func (s *Server) sendMsgWithRedo(args *pb.MsgArgs, sub *subcription, timeout int) (*pb.MsgReply, error) {
	if args.Redo >= int32(config.SrvConf.OperationRedoNum) {
		return nil, errors.New("match max redo")
	}

	reply, err := s.sendMsg(args, sub, timeout)
	if err != nil {
		logger.Errorf("sendMsg failed, try to resend: %v", err)
		args.Redo++
		return s.sendMsgWithRedo(args, sub, timeout)
	}
	return reply, nil
}

func (s *Server) sendMsg(args *pb.MsgArgs, sub *subcription, timeout int) (*pb.MsgReply, error) {
	logger.Debugf("send a msg: %v", args)
	cli := pb.NewClientClient(sub.clients[args.Suber])
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.ProcessMsg(ctx, args)
}

func (s *Server) batchPull(ctx context.Context, args *pb.PullArgs) (*pb.PullReply, error) {
	reply := &pb.PullReply{}
	pua := &msg.PullArg{
		Topic:     args.Topic,
		Partition: int(args.Partition),
		Subname:   args.Subscription,
		Bufsize:   int(args.BufSize),
	}

	pkey := fmt.Sprintf(partitionKey, pua.Topic, pua.Partition)
	pNode := s.ps[pkey]

	skey := fmt.Sprintf(subcriptionKey, pua.Topic, pua.Partition, pua.Subname)
	exSub := s.Sl.Subs[skey]

	go pua.CheckTimeout(int(args.Timeout))

	for {
		if _, ok := <-pua.Timeout; ok {
			break
		}
		if pNode.pNode.Mnum > exSub.Data.PushOffset || pua.Bufsize <= 0 {
			pua.Full <- true
			break
		}

		pushedNum := 0
		var msgs []*msg.MsgData
		exSub.mu.Lock()
		i := exSub.Data.PushOffset + 1
		for i <= pNode.pNode.Mnum && pushedNum%defaultSendSize < defaultSendSize {
			if pua.Bufsize <= 0 {
				pua.Full <- true
				break
			}
			var m *msg.MsgData
			key := fmt.Sprintf(msgKey, pua.Topic, pua.Partition, i)
			if en, ok := pNode.msgs.Load(key); ok {
				m = en.(*msg.MsgData)
			} else {
				ms, err := s.GetMsg(pua, i)
				if err != nil {
					reply.Error = err.Error()
					return reply, err
				}
				m = ms
			}
			msgs = append(msgs, m)
			i++
			pushedNum++
			if pNode.pNode.PushOffset < i {
				pNode.pNode.PushOffset = i
			}
		}

		// batch
		c := pb.NewClientClient(exSub.clients[args.Name])
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		payload, err := json.Marshal(msgs)
		if err != nil {
			return reply, err
		}
		msgArgs := &pb.MsgArgs{Payload: string(payload)}

		c.ProcessMsg(ctx, msgArgs)
		exSub.Data.PushOffset += uint64(len(msgs))
	}
	return reply, nil
}

func (s *Server) MsgAck(ctx context.Context, args *pb.MsgAckArgs) (*pb.MsgAckReply, error) {
	reply := &pb.MsgAckReply{}
	path := fmt.Sprintf(partitionKey, args.Topic, args.Partition)
	p, ok := s.partitions.Load(path)
	pData := new(partitionData)
	if !ok {
		logger.Errorf("s.partitions.Load failed: %v", path)
	} else {
		pData = p.(*partitionData)
	}

	pData.mu.Lock()
	if pData.pNode.AckOffset < args.AckOffset {
		pData.pNode.AckOffset = args.AckOffset
		if err := rc.ZkCli.UpdatePartition(pData.pNode); err != nil {
			logger.Errorf("UpdatePartition failed: %v", err)
		}
	}
	pData.mu.Unlock()

	skey := fmt.Sprintf(subcriptionKey, args.Topic, args.Partition, args.Subscription)
	exSub := s.Sl.Subs[skey]
	exSub.mu.Lock()
	if exSub.Data.AckOffset > args.AckOffset {
		exSub.Data.AckOffset = args.AckOffset
	}
	exSub.mu.Unlock()

	//TODO: retry ?
	return reply, nil
}

func (s *Server) reTry() {

}

func (s *Server) ProcessUnsub(ctx context.Context, args *pb.UnSubscribeArgs) (*pb.UnSubscribeReply, error) {
	reply := &pb.UnSubscribeReply{}
	sub := NewSubcription()
	sub.Data.Meta.TopicName = args.Topic
	sub.Data.Meta.Partition = int(args.Partition)
	sub.Data.Meta.Name = args.Subscription

	var exSub *subcription
	name := args.Name
	key := fmt.Sprintf(subcriptionKey, sub.Data.Meta.TopicName, sub.Data.Meta.Partition, sub.Data.Meta.Name)
	if sub, ok := s.Sl.Subs[key]; ok {
		exSub = sub
	} else {
		isExists, err := rc.ZkCli.IsSubcriptionExist(&sub.Data.Meta)
		if err != nil {
			return nil, err
		}
		if isExists {
			existSnode, err := rc.ZkCli.GetSub(&sub.Data.Meta)
			if err != nil {
				return nil, err
			}
			existSdata, err := s.GetSubcription(existSnode)
			if err != nil {
				return nil, err
			}
			exSub = existSdata
			s.Sl.Subs[name] = exSub
		}
	}

	if exSub != nil {
		if _, ok := exSub.Data.Subers[name]; !ok {
			return nil, errors.New("not exist in this subcription")
		}

		delete(s.Sl.Subs, name)
		if err := s.PutSubcription(exSub); err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("subcription not exist")
	}
	return reply, nil
}

func (s *Server) ProcessPub(ctx context.Context, args *pb.PublishArgs) (*pb.PublishReply, error) {
	logger.Infof("Receive Publish rq from %v", args)
	reply := &pb.PublishReply{}
	pNode := new(partitionData)
	path := fmt.Sprintf(partitionKey, args.Topic, args.Partition)
	if v, ok := s.partitions.Load(path); ok {
		pNode = v.(*partitionData)
	} else {
		isExists, err := rc.ZkCli.IsPartitionExists(args.Topic, int(args.Partition))
		if err != nil {
			return reply, err
		}
		if isExists {
			pNode.pNode, err = rc.ZkCli.GetPartition(args.Topic, int(args.Partition))
			if err != nil {
				logger.Errorf("GetPartition failed: %v", err)
			}
			s.partitions.Store(path, pNode)
		} else {
			logger.Errorf("there is no this topic/partition %v/%v", args.Topic, args.Partition)
			return reply, errors.New("404")
		}
	}

	// todo: check

	pNode.mu.Lock()
	mData := msg.MsgData{
		Msid:    pNode.pNode.Mnum + 1,
		Mid:     args.Mid,
		Payload: args.Payload,
	}
	pa := &msg.PubArg{
		Topic:     args.Topic,
		Partition: int(args.Partition),
		Mid:       args.Mid,
	}
	if _, err := s.PutMsg(pa, mData); err != nil {
		return reply, err
	}
	pNode.pNode.Mnum += 1
	reply.Msid = pNode.pNode.Mnum
	if err := rc.ZkCli.UpdatePartition(pNode.pNode); err != nil {
		logger.Errorf("UpdatePartition: %v", err)
		p, _ := rc.ZkCli.GetPartition(pNode.pNode.TopicName, pNode.pNode.ID)
		logger.Debugln(*pNode.pNode, " ", *p)
		return reply, err
	}
	logger.Infof("persist a message: %v %v", pa, mData)
	pNode.mu.Unlock()

	return reply, nil
}

func (s *Server) GetTopicInfo(ctx context.Context, args *pb.GetTopicInfoArgs) (*pb.GetTopicInfoReply, error) {
	logger.Infof("Receive GetTopicInfo rq from %v", args)
	reply := &pb.GetTopicInfoReply{}
	tNode, err := rc.ZkCli.GetTopic(args.Topic)
	if err != nil {
		logger.Errorf("GetTopic failed: %v", err)
		return reply, errors.New("404")
	}

	reply.PartitionNum = int32(tNode.Pnum)
	logger.Debugf("GetTopicInfo reply: %v", reply)
	return reply, nil
}

func (s *Server) ClientAlive(conn *grpc.ClientConn, Cargs pb.ConnectArgs) {
	count := 0
	cli := pb.NewClientClient(conn)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(config.SrvConf.RpcTimeout))
		defer cancel()
		args := &pb.AliveCheckArgs{}
		_, err := cli.AliveCheck(ctx, args)
		if err != nil {
			// if ok := checkTimeout(err); ok {
			count++
			if count >= config.SrvConf.TimeoutTimes {
				rc.ZkCli.DeleteLeadPuber(Cargs.Topic, int(Cargs.Partition))
				logger.Infof("not alive: %v, err: %v", Cargs, err)
				return
			}
			// }
		} else {
			count = 0
		}
		time.Sleep(time.Second * time.Duration(config.SrvConf.HeartBeatInterval))
	}
}

func (s *Server) SuberAlive(conn *grpc.ClientConn, Sargs pb.SubscribeArgs) {
	count := 0
	cli := pb.NewClientClient(conn)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(config.SrvConf.RpcTimeout))
		defer cancel()
		args := &pb.AliveCheckArgs{}
		_, err := cli.AliveCheck(ctx, args)
		if err != nil {
			count++
			if count >= config.SrvConf.TimeoutTimes {
				if err1 := rc.ZkCli.DeleteLeadSuber(Sargs.Topic, int(Sargs.Partition), Sargs.Subscription); err1 != nil {
					logger.Errorf("DeleteLeadSuber failed: %v", err1)
				}
				logger.Infof("not alive: %v, err: %v", Sargs, err)
				return
			}
		} else {
			count = 0
		}
		time.Sleep(time.Second * time.Duration(config.SrvConf.HeartBeatInterval))
	}
}

func checkTimeout(err error) bool {
	statusErr, ok := status.FromError(err)
	if ok && (statusErr.Code() == codes.DeadlineExceeded || statusErr.Code() == codes.Canceled) {
		return true
	}
	return false
}
