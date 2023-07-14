## MxcMQ

MxcMQ 是一个使用 golang 编写的高可用消息队列，通过异步方式进行通信，发送和接收消息，提供统一的消费模型，支持队列和发布订阅两种消费模式。。

## 特点

+ 使用异步通信方式，支持队列和发布订阅两种消费模式和备灾模式。
+ 将元数据存储在 ZooKeeper 中，将数据存储在 etcd 中以实现存算分离。
+ 使用 Broker 实现无状态，轻量化的上下线操作。
+ 基于 Dynamic Push/Pull 模型，优化了消费端的性能表现。
+ 引入抽象的 Bundle 层，减少了主题迁移的需求，优化了负载均衡的性能。
+ 实现了一致性哈希，可以根据节点状态进行细粒度负载均衡。

## 安装和测试
