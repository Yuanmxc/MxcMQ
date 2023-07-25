package MxcMQClient

import (
	pb "MxcMQ-Client/proto"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Client struct {
	Partition           int32
	OperationMaxRedoNum int32
	conn                *grpc.ClientConn
	msgCh               chan Msg
	pb.UnimplementedClientServer
}

type Msg struct {
	Topic     string
	Partition int
	Mid       int64
	Msid      uint64
	Data      []byte
}

type PublishMode int32

type SubscribeMode int32

type RouteMode int32

const (
	Puber = iota
	PartPuber
	Suber
)

const (
	PMode_Exclusive     PublishMode = 0
	PMode_WaitExclusive PublishMode = 1
	PMode_Shared        PublishMode = 2
)

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

func (c *Client) Msg(ctx context.Context, args *pb.MsgArgs) (*pb.MsgReply, error) {
	reply := &pb.MsgReply{}

	return reply, nil
}

func (c *Client) Listen(cliUrl string) error {
	lis, err := net.Listen("tcp", cliUrl)
	if err != nil {
		return err
	}

	s := grpc.NewServer()
	pb.RegisterClientServer(s, &Client{})

	go func() {
		s.Serve(lis)
	}()
	//if err := s.Serve(lis); err != nil {
	//	return err
	//}
	return nil
}

func (c *Client) Connect(srvUrl string, args *pb.ConnectArgs) (string, error) {
	Url, err := c.lookup(srvUrl, args.Name, args.Topic, args.Partition, args.Timeout)
	if err != nil {
		return "", err
	}

	conn, err := grpc.Dial(Url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "nil", err
	}
	c.conn = conn
	reply, err := c.Connect2serverWithRedo(args, int(args.Timeout))
	if err != nil {
		return "", err
	}
	return reply.Name, nil
}

func (c *Client) lookup(srvUrl string, name string, topic string, partition int32, ConnectTimeout int32) (string, error) {
	Url := ""
	lconn, err := grpc.Dial(srvUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", err
	}
	c.conn = lconn

	lookUpArgs := &pb.LookUpArgs{
		Name:      name,
		Topic:     topic,
		Partition: partition,
		Redo:      0,
	}
	lookUpReply, err := c.LookUpForServer(lookUpArgs, int(ConnectTimeout))
	lconn.Close()
	if err != nil {
		if c.GetErrorString(err) == "need to connect leader to alloc" {
			//alloc
			aconn, err := grpc.Dial(srvUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return "", err
			}
			c.conn = aconn
			allocArgs := &pb.RequestAllocArgs{
				Topic:     topic,
				Partition: partition,
				Redo:      0,
			}
			allocReply, err := c.RequestAllocWithRedo(allocArgs, int(ConnectTimeout))
			if err != nil {
				return "", err
			} else {
				Url = allocReply.Url
			}
		}
		return "", err
	} else {
		Url = lookUpReply.Url
	}

	return Url, nil
}

func (c *Client) LookUpWithRedo(args *pb.LookUpArgs, timeout int) (*pb.LookUpReply, error) {
	if args.Redo >= c.OperationMaxRedoNum {
		return nil, errors.New("match max redo")
	}

	reply, err := c.LookUpForServer(args, timeout)
	if err != nil {
		if ok := c.CheckTimeout(err); ok {
			args.Redo++
			return c.LookUpWithRedo(args, timeout)
		}
		return nil, err
	}
	return reply, nil
}

func (c *Client) LookUpForServer(args *pb.LookUpArgs, timeout int) (*pb.LookUpReply, error) {
	cli := pb.NewServerClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.LookUp(ctx, args)
}

func (c *Client) RequestAllocWithRedo(args *pb.RequestAllocArgs, timeout int) (*pb.RequestAllocReply, error) {
	if args.Redo >= c.OperationMaxRedoNum {
		return nil, errors.New("match max redo")
	}
	reply, err := c.RequestAlloc2LeadServer(args, timeout)
	if err != nil {
		if ok := c.CheckTimeout(err); ok {
			args.Redo++
			return c.RequestAllocWithRedo(args, timeout)
		}
		return nil, err
	}

	return reply, nil
}

func (c *Client) RequestAlloc2LeadServer(args *pb.RequestAllocArgs, timeout int) (*pb.RequestAllocReply, error) {
	cli := pb.NewServerClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.RequestAlloc(ctx, args)
}

func (c *Client) Connect2serverWithRedo(args *pb.ConnectArgs, timeout int) (*pb.ConnectReply, error) {
	if args.Redo >= c.OperationMaxRedoNum {
		return nil, errors.New("match max redo")
	}

	reply, err := c.Connet2server(args, timeout)
	if err != nil {
		if ok := c.CheckTimeout(err); ok {
			args.Redo++
			return c.Connect2serverWithRedo(args, timeout)
		}
		return nil, err
	}
	return reply, nil
}

func (c *Client) Connet2server(args *pb.ConnectArgs, timeout int) (*pb.ConnectReply, error) {
	cli := pb.NewServerClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.Connect(ctx, args)
}

func (c *Client) Push2serverWithRedo(args *pb.PublishArgs, timeout int) (*pb.PublishReply, error) {
	if args.Redo >= c.OperationMaxRedoNum {
		return nil, errors.New("match max redo")
	}

	reply, err := c.Push2server(args, timeout)
	if err != nil {
		if ok := c.CheckTimeout(err); ok {
			args.Redo++
			return c.Push2serverWithRedo(args, timeout)
		}
	}
	return reply, nil
}

func (c *Client) Push2server(args *pb.PublishArgs, timeout int) (*pb.PublishReply, error) {
	cli := pb.NewServerClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.ProcessPub(ctx, args)
}

func (c *Client) SubscribeWithRedo(args *pb.SubscribeArgs, timeout int) (*pb.SubscribeReply, error) {
	if args.Redo >= c.OperationMaxRedoNum {
		return nil, errors.New("match max redo")
	}

	reply, err := c.Subscribe(args, timeout)
	if err != nil {
		if ok := c.CheckTimeout(err); ok {
			args.Redo++
			return c.SubscribeWithRedo(args, timeout)
		}
	}
	return reply, nil
}

func (c *Client) Subscribe(args *pb.SubscribeArgs, timeout int) (*pb.SubscribeReply, error) {
	cli := pb.NewServerClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.ProcessSub(ctx, args)
}

func (c *Client) PullWithRedo(args *pb.PullArgs, timeout int) (*pb.PullReply, error) {
	if args.Redo >= c.OperationMaxRedoNum {
		return nil, errors.New("match max redo")
	}

	reply, err := c.Pull(args, timeout)
	if err != nil {
		if ok := c.CheckTimeout(err); ok {
			args.Redo++
			return c.PullWithRedo(args, timeout)
		}
	}
	return reply, nil
}

func (c *Client) Pull(args *pb.PullArgs, timeout int) (*pb.PullReply, error) {
	cli := pb.NewServerClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.ProcessPull(ctx, args)
}

func (c *Client) GetTopicWithRedo(args *pb.GetTopicInfoArgs, timeout int) (*pb.GetTopicInfoReply, error) {
	if args.Redo >= c.OperationMaxRedoNum {
		return nil, errors.New("match max redo")
	}

	reply, err := c.GetTopic(args, timeout)
	if err != nil {
		if ok := c.CheckTimeout(err); ok {
			args.Redo++
			return c.GetTopic(args, timeout)
		}
	}
	return reply, nil
}

func (c *Client) GetTopic(args *pb.GetTopicInfoArgs, timeout int) (*pb.GetTopicInfoReply, error) {
	cli := pb.NewServerClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.GetTopicInfo(ctx, args)
}

func (c *Client) UnSubscribeWithRedo(args *pb.UnSubscribeArgs, timeout int) (*pb.UnSubscribeReply, error) {
	if args.Redo >= c.OperationMaxRedoNum {
		return nil, errors.New("match max redo")
	}

	reply, err := c.UnSubscribe(args, timeout)
	if err != nil {
		if ok := c.CheckTimeout(err); ok {
			args.Redo++
			return c.UnSubscribeWithRedo(args, timeout)
		}
	}
	return reply, nil
}

func (c *Client) UnSubscribe(args *pb.UnSubscribeArgs, timeout int) (*pb.UnSubscribeReply, error) {
	cli := pb.NewServerClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.ProcessUnSub(ctx, args)
}

func (c *Client) CheckTimeout(err error) bool {
	statusErr, ok := status.FromError(err)
	if ok && statusErr.Code() == codes.DeadlineExceeded {
		return true
	}
	return false
}

func (c *Client) GetErrorString(err error) string {
	statusErr, ok := status.FromError(err)
	if !ok {
		return ""
	}
	return statusErr.Message()
}

func (c *Client) DisConnect() error {
	return c.conn.Close()
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (c *Client) ProcessMsg(ctx context.Context, args *pb.MsgArgs) (*pb.MsgReply, error) {
	fmt.Printf("Get msg from %v", args)
	reply := &pb.MsgReply{}
	msg := Msg{
		Topic:     args.Topic,
		Partition: int(args.Partition),
		Mid:       args.Mid,
		Msid:      args.Msid,
		Data:      []byte(args.Payload),
	}
	c.msgCh <- msg
	return reply, nil
}
