# MxcMQ

## 介绍

MxcMQ 是一个使用 golang 编写的高可用消息队列，通过异步方式进行通信，发送和接收消息，提供统一的消费模型，支持队列和发布订阅两种消费模式。。

## 主要特点

+ 使用异步通信方式，支持队列和发布订阅两种消费模式和备灾模式。
+ 将元数据存储在 ZooKeeper 中，将数据存储在 etcd 中以实现存算分离。
+ 使用 Broker 实现无状态，轻量化的上下线操作。
+ 基于 Dynamic Push/Pull 模型，优化了消费端的性能表现。
+ 引入抽象的 Bundle 层，减少了主题迁移的需求，优化了负载均衡的性能。
+ 实现了一致性哈希，可以根据节点状态进行细粒度负载均衡。

## 安装与运行

1. 安装工具和依赖：
    - go
    - zookeeper
    - etcd

2. 编译运行：

    ```bash
    go build # 编译生成可执行文件
    ./MxcMQ start -c ./config/config.yaml # 指定参数和配置文件
    ```

## 客户端

### 发布者

#### 创建配置

- 发布方式
- 超时时间
- 异步队列大小
  

...

### 样例
``` go
opts := []MxcMQ.PubOption{
	MxcMQ.WithpMode(MxcMQ.PMode_Shared),
}
puber, err := MxcMQ.NewPublisher("localhost:8888", "localhost", 7777, "TestTopic", opt...)
if err != nil {
	panic(err)
}

if err := puber.Connect(); err != nil {
	panic(err)
}

msg := &MxcMQ.Msg{
	Data: []byte("hello MxcMQ")
}
if err := puber.Publish(msg); err != nil {
	panic(err)
}
```

### 订阅者

#### 创建配置

订阅者：
- 超时时间
- 重传次数

...

订阅：
- 订阅方式
- 订阅分区
- 订阅位点
- 接收队列大小

...
### 样例
``` go
suber := cli.NewSubscriber("localhost:8888", "localhost", 8877, "s2")

opts := []cli.SubscipOption{
	cli.WithspMode(cli.SMode_Failover),
}
subscription, err := suber.Subscribe("testSubscrption1" "testTopic", opt...)
if err != nil {
  	panic(err)
}

msg, err := subscription.Receive()
if err != nil {
	fmt.Println(msg)
	subscription.MsgAck(msg)
}
```
