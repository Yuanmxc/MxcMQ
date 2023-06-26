package RegistraionCenter

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"MxcMQ/config"
	"MxcMQ/logger"

	ct "MxcMQ/collect"

	"github.com/samuel/go-zookeeper/zk"
)

var ZkCli *ZkClient

var (
	BnodePath     = "%v/%v"               // BrokerRoot/BrokerName
	TnodePath     = "%v/%v"               // TopicRoot/TopicName
	BunodePath    = "%v/bundle%v"         // BundleRoot/BundleName
	PnodePath     = "%v/%v/p%v"           // TopicRoot/TopicName/PartitionName
	SnodePath     = "%v/%v/p%v/%v"        // TopicRoot/TopicName/PartitionName/SubcriptionName
	LeadPuberPath = "%v/%v/p%v/leader"    // TopicRoot/TopicName/PartitionName
	LeadSuberPath = "%v/%v/p%v/%v/leader" // TopicRoot/TopicName/PartitionName/SubcriptionName
)

type ZkClient struct {
	ZkServers      []string
	ZkRoot         string
	ZkBrokerRoot   string
	ZkTopicRoot    string
	ZkBundleRoot   string
	LeadBrokerPath string
	SessionTimeout int
	Conn           *zk.Conn
}

type BrokerNode struct {
	Name      string `json:"name"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Pnum      int    `json:"pnum"`
	Version   int32
	Load      ct.BrokerUsage
	LoadIndex float64
}

type TopicNode struct {
	Name       string `json:"name"`
	Pnum       int    `json:"pnum"`
	PulishMode int
}

type PartitionNode struct {
	ID         int    `json:"name"`
	TopicName  string `json:"topicName"`
	Mnum       uint64 `json:"mnum"`
	AckOffset  uint64 `json:"ackoffset"`
	PushOffset uint64 `json:"pushoffset"`
	Url        string
	Version    int32
}

type BundleNode struct {
	ID        int
	Start     uint32
	End       uint32
	BrokerUrl string
	Version   int32
}

type SubcriptionNode struct {
	Name      string
	TopicName string
	Partition int
	Subtype   int
}

type LeaderNode struct {
	LeaderUrl string
}

func RcInit() {
	ZkCli, _ = NewClient()
	ZkCli.preRoot()
	logger.Infoln("zk init over")
}

func (c *ZkClient) preRoot() {
	err := c.ensureExist(c.ZkRoot)
	if err != nil {
		panic(err)
	}
	err = c.ensureExist(c.ZkBrokerRoot)
	if err != nil {
		panic(err)
	}
	err = c.ensureExist(c.ZkTopicRoot)
	if err != nil {
		panic(err)
	}
	err = c.ensureExist(c.ZkBundleRoot)
	if err != nil {
		panic(err)
	}
}

func callback(e zk.Event) {}

func NewClient() (*ZkClient, error) {
	c := &ZkClient{
		ZkServers:      config.ZkConf.Host,
		ZkRoot:         config.ZkConf.Root,
		ZkBrokerRoot:   config.ZkConf.BrokerRoot,
		ZkTopicRoot:    config.ZkConf.TopicRoot,
		ZkBundleRoot:   config.ZkConf.BundleRoot,
		LeadBrokerPath: config.ZkConf.LeadBrokerRoot,
		SessionTimeout: config.ZkConf.SessionTimeout,
	}

	Conn, _, err := zk.Connect(c.ZkServers, time.Duration(c.SessionTimeout)*time.Second)
	if err != nil {
		panic("connect zk failed.")
	}
	c.Conn = Conn

	return c, nil
}

func NewClientWithCallback(cb func(e zk.Event)) (*ZkClient, error) {
	c := &ZkClient{
		ZkServers:      config.ZkConf.Host,
		ZkRoot:         config.ZkConf.Root,
		ZkBrokerRoot:   config.ZkConf.BrokerRoot,
		ZkTopicRoot:    config.ZkConf.TopicRoot,
		ZkBundleRoot:   config.ZkConf.BundleRoot,
		SessionTimeout: config.ZkConf.SessionTimeout,
	}

	eventCallbackOption := zk.WithEventCallback(cb)
	conn, _, err := zk.Connect(c.ZkServers, time.Second*time.Duration(config.ZkConf.SessionTimeout), eventCallbackOption)
	if err != nil {
		return nil, err
	}
	c.Conn = conn

	return c, nil
}

func (c *ZkClient) RegisterBnode(bnode BrokerNode) error {
	path := fmt.Sprintf(BnodePath, c.ZkBrokerRoot, bnode.Name)
	data, err := json.Marshal(bnode)
	if err != nil {
		return err
	}

	return c.registerTemNode(path, data)
}

func (c *ZkClient) RegisterTnode(tnode *TopicNode) error {
	path := fmt.Sprintf(TnodePath, c.ZkTopicRoot, tnode.Name)
	data, err := json.Marshal(tnode)
	if err != nil {
		return err
	}
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterPnode(pnode *PartitionNode) error {
	path := fmt.Sprintf(PnodePath, c.ZkTopicRoot, pnode.TopicName, pnode.ID)
	data, err := json.Marshal(pnode)
	if err != nil {
		return err
	}
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterBunode(bunode *BundleNode) error {
	path := fmt.Sprintf(BunodePath, c.ZkBundleRoot, bunode.ID)
	data, err := json.Marshal(bunode)
	if err != nil {
		return err
	}
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterSnode(snode *SubcriptionNode) error {
	path := fmt.Sprintf(SnodePath, c.ZkTopicRoot, snode.TopicName, snode.Partition, snode.Name)
	data, err := json.Marshal(snode)
	if err != nil {
		return err
	}
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterLeadPuberNode(topic string, partition int) error {
	path := fmt.Sprintf(LeadPuberPath, c.ZkTopicRoot, topic, partition)
	return c.RegisterNode(path, []byte{65})
}

func (c *ZkClient) RegisterLeadSuberNode(topic string, partition int, subscription string) error {
	path := fmt.Sprintf(LeadSuberPath, c.ZkTopicRoot, topic, partition, subscription)
	return c.registerTemNode(path, []byte{65})
}

func (c *ZkClient) RegisterLeadBrokernode(lNode *LeaderNode) error {
	data, err := json.Marshal(lNode)
	if err != nil {
		return err
	}
	return c.registerTemNode(c.LeadBrokerPath, data)
}

func (c *ZkClient) RegisterNode(path string, data []byte) error {
	_, err := c.Conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	return err
}

func (c *ZkClient) registerTemNode(path string, data []byte) error {
	//todo: choose one?
	// _, err := c.Conn.CreateProtectedEphemeralSequential(path, data, zk.WorldACL(zk.PermAll))
	_, err := c.Conn.Create(path, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	return err
}

func (c *ZkClient) RegisterLeadBrokerWatch() (bool, <-chan zk.Event, error) {
	return c.registerWatcher(c.LeadBrokerPath)
}

func (c *ZkClient) RegisterLeadPuberWatch(topic string, partition int) (bool, <-chan zk.Event, error) {
	path := fmt.Sprintf(LeadPuberPath, c.ZkTopicRoot, topic, partition)
	return c.registerWatcher(path)
}

func (c *ZkClient) RegisterLeadSuberWatch(topic string, partition int, subscription string) (bool, <-chan zk.Event, error) {
	path := fmt.Sprintf(LeadSuberPath, c.ZkTopicRoot, topic, partition, subscription)
	return c.registerWatcher(path)
}

func (c *ZkClient) registerWatcher(path string) (bool, <-chan zk.Event, error) {
	isExists, _, ch, err := c.Conn.ExistsW(path)
	return isExists, ch, err
}

func (c *ZkClient) RegisterChildrenWatcher(path string) ([]string, <-chan zk.Event, error) {
	znodes, _, ch, err := c.Conn.ChildrenW(path)
	return znodes, ch, err
}

func (c *ZkClient) GetBrokers(topic string) ([]*PartitionNode, error) {
	var pNodes []*PartitionNode
	path := fmt.Sprintf(TnodePath, c.ZkTopicRoot, topic)

	isExists, err := c.IsTopicExists(topic)
	if err != nil {
		return nil, err
	}

	if !isExists {
		//Todo: how to create? p / no p
		//default: no p
		err := c.createTopic(topic)
		if err != nil {
			return nil, err
		}
	}

	znodes, _, err := c.Conn.Children(path)
	if err != nil {
		return nil, err
	}

	for _, znode := range znodes {
		pPath := path + "/" + znode
		data, _, err := c.Conn.Get(pPath)
		if err != nil {
			return nil, err
		}
		pNode := &PartitionNode{}
		err = json.Unmarshal(data, pNode)
		if err != nil {
			return nil, err
		}
		pNodes = append(pNodes, pNode)
	}

	return pNodes, nil
}

func (c *ZkClient) GetBroker(topic string, partition int) (*PartitionNode, error) {
	path := fmt.Sprintf(PnodePath, c.ZkTopicRoot, topic, partition)
	isExists, err := c.IsPartitionExists(topic, partition)
	if err != nil {
		return nil, err
	}
	if !isExists {
		return nil, errors.New("znode is not exists")
	}

	data, _, err := c.Conn.Get(path)
	if err != nil {
		return nil, err
	}

	pNode := &PartitionNode{}
	err = json.Unmarshal(data, pNode)
	if err != nil {
		return nil, err
	}

	return pNode, nil
}

func (c *ZkClient) GetTopic(topic string) (*TopicNode, error) {
	path := fmt.Sprintf(TnodePath, c.ZkTopicRoot, topic)
	data, _, err := c.Conn.Get(path)
	if err != nil {
		return nil, err
	}

	tNode := &TopicNode{}
	if err = json.Unmarshal(data, tNode); err != nil {
		return nil, err
	}
	return tNode, nil
}

func (c *ZkClient) GetSub(snode *SubcriptionNode) (*SubcriptionNode, error) {
	path := fmt.Sprintf(SnodePath, c.ZkTopicRoot, snode.TopicName, snode.Partition, snode.Name)
	data, _, err := c.Conn.Get(path)
	if err != nil {
		return nil, err
	}

	sNode := &SubcriptionNode{}
	if err = json.Unmarshal(data, sNode); err != nil {
		return nil, err
	}
	return sNode, nil
}

func (c *ZkClient) GetPartition(topic string, partition int) (*PartitionNode, error) {
	pNode := &PartitionNode{}
	return pNode, nil
}

func (c *ZkClient) GetBundles(bnum int) ([]*BundleNode, error) {
	var bundles []*BundleNode
	for bnum > 0 {
		bNode, err := c.GetBundle(bnum)
		if err != nil {
			return nil, err
		}
		bundles = append(bundles, bNode)
		bnum--
	}
	return bundles, nil
}

func (c *ZkClient) GetBundle(id int) (*BundleNode, error) {
	path := fmt.Sprintf(BunodePath, config.ZkConf.BundleRoot, id)
	data, _, err := c.Conn.Get(path)
	if err != nil {
		return nil, err
	}

	bNode := &BundleNode{}
	if err = json.Unmarshal(data, bNode); err != nil {
		return nil, err
	}
	return bNode, nil
}

func (c *ZkClient) GetLeader() (*LeaderNode, error) {
	data, _, err := c.Conn.Get(c.LeadBrokerPath)
	if err != nil {
		return nil, err
	}

	lNode := &LeaderNode{}
	if err = json.Unmarshal(data, lNode); err != nil {
		return nil, err
	}
	return lNode, nil
}

func (c *ZkClient) GetAllBrokers() ([]*BrokerNode, error) {
	var brokers []*BrokerNode
	znodes, _, err := c.Conn.Children(c.ZkBrokerRoot)
	if err != nil {
		return nil, err
	}

	for _, znode := range znodes {
		bPath := c.ZkBrokerRoot + "/" + znode
		data, _, err := c.Conn.Get(bPath)
		if err != nil {
			return nil, err
		}
		bNode := &BrokerNode{}
		err = json.Unmarshal(data, bNode)
		if err != nil {
			return nil, err
		}
		brokers = append(brokers, bNode)
	}
	return brokers, nil
}

func (c *ZkClient) IsPubersExists(topic string, partition int) (bool, error) {
	path := fmt.Sprintf(PnodePath, c.ZkTopicRoot, topic, partition)
	pubers, _, err := c.Conn.Children(path)
	if err != nil {
		return false, err
	}
	return len(pubers) == 0, nil
}

func (c *ZkClient) IsSubersExists(topic string, partition int, subscription string) (bool, error) {
	path := fmt.Sprintf(SnodePath, c.ZkTopicRoot, topic, partition, subscription)
	subers, _, err := c.Conn.Children(path)
	if err != nil {
		return false, err
	}
	return len(subers) == 0, nil
}

// func (c *ZkClient) getZnode(path string) (){

// }

func (c *ZkClient) ensureExist(name string) error {
	isExists, _, err := c.Conn.Exists(name)
	if err != nil && err != zk.ErrNoNode {
		return err
	}
	if !isExists {
		_, err := c.Conn.Create(name, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}

	return nil
}

func (c *ZkClient) IsBrokerExists(name string) (bool, error) {
	path := fmt.Sprintf(BnodePath, c.ZkBrokerRoot, name)
	return c.isZnodeExists(path)
}

func (c *ZkClient) IsTopicExists(topic string) (bool, error) {
	// path := c.ZkTopicRoot + "/" + topic
	// isExists, _, err := c.Conn.Exists(path)
	// if err != nil {
	// 	return false, err
	// }

	// return isExists, nil

	// Todo: above is needed?
	return c.IsPartitionExists(topic, 1)
}

func (c *ZkClient) IsPartitionExists(topic string, partition int) (bool, error) {
	path := c.ZkTopicRoot + "/" + topic + "p" + strconv.Itoa(partition)
	return c.isZnodeExists(path)
}

func (c *ZkClient) IsSubcriptionExist(snode *SubcriptionNode) (bool, error) {
	path := fmt.Sprintf(SnodePath, c.ZkTopicRoot, snode.TopicName, snode.Partition, snode.Name)
	return c.isZnodeExists(path)
}

func (c *ZkClient) IsLeaderExist() (bool, error) {
	return c.isZnodeExists(c.LeadBrokerPath)
}

func (c *ZkClient) isZnodeExists(path string) (bool, error) {
	isExists, _, err := c.Conn.Exists(path)
	return isExists, err
}

func (c *ZkClient) createTopic(topic string) error {
	return c.createPartitionTopic(topic, 1)
}

func (c *ZkClient) createPartitionTopic(topic string, partition int) error {
	tPath := c.ZkTopicRoot + "/" + topic
	tData := TopicNode{
		Name: topic,
		Pnum: 1,
	}
	tdata, _ := json.Marshal(tData)
	_, err := c.Conn.Create(tPath, tdata, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	for i := 1; i <= partition; i++ {
		pPath := tPath + "/" + "p" + strconv.Itoa(i)
		pData := PartitionNode{
			ID:        i,
			TopicName: topic,
		}
		pdata, _ := json.Marshal(pData)
		_, err = c.Conn.Create(pPath, pdata, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ZkClient) UpdatePartition(pNode *PartitionNode) error {
	path := fmt.Sprintf(PnodePath, c.ZkTopicRoot, pNode.TopicName, pNode.ID)
	data, err := json.Marshal(pNode)
	if err != nil {
		return err
	}

	_, err = c.Conn.Set(path, data, pNode.Version)
	pNode.Version++
	return err
}

func (c *ZkClient) UpdateBroker(bNode *BrokerNode) error {
	path := fmt.Sprintf(BnodePath, c.ZkBrokerRoot, bNode.Name)
	data, err := json.Marshal(bNode)
	if err != nil {
		return err
	}
	_, err = c.Conn.Set(path, data, bNode.Version)
	bNode.Version++
	return err
}

func (c *ZkClient) UpdateBundle(buNode *BundleNode) error {
	path := fmt.Sprintf(BunodePath, c.ZkBundleRoot, buNode.ID)
	data, err := json.Marshal(buNode)
	if err != nil {
		return err
	}
	_, err = c.Conn.Set(path, data, buNode.Version)
	buNode.Version++
	return err
}

func (c *ZkClient) Close() {
	c.Conn.Close()
}
