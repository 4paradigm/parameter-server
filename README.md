# pico-ps

pico-ps 是一个基于 prpc 的高性能参数服务器框架，支持 pull, push, load, dump 等操作，支持 restore dead node ，支持运行时增删节点，负载均衡，支持高可用。

pico-ps 本身提供了一个基于 hash table 的参数服务器，只需定义如何 apply gradients，如何序列化参数，就可以使用 PS 完成训练和预估。除此之外，用户也可以基于 pico-ps 框架定制自己的存储结构，消息格式等。

## build

[overview] (documents/overview.md)

[operator] (documents/operator.md)

[availability] (documents/availability.md)
