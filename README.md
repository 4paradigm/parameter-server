# pico-ps

pico-ps是一个基于prpc的高性能参数服务器框架，支持pull，push，load，dump等操作，支持restore dead node，支持增删节点，负载均衡，支持高可用。
pico-ps本身提供了一个基于hash table的参数服务器，只需定义如何apply gradients，如何序列化参数，就可以使用PS完成训练和预估。除此之外，用户也可以基于pico-ps框架定制自己的存储结构，消息格式等。

