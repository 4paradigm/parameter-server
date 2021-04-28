# Operator

## StorageOperator

StorageOperator 没有对应 Handler ，StorageOperator 在 create_storage 时直接指定，由 server 自动调用，用来初始化 server 上 storage 的存储空间，另外还定义了如何统计 storage 的内存占用等信息。

## PushOperator

PushOperator 可以实现 async push （用于异步训练）和 sync push （用于同步训练）两种 push 操作，sync push 还需要分别实现 push 梯度和 apply 梯度， PushOperator 中可以定义一系列 create storage 方法，包括用来存储梯度的 delta storage 和训练增量的 increment storage。PushOperator 也可以实现为直接复制参数，如 DirectPushOperator ，这类 PushOperator 主要用于 server 之间 shuffle 数据，一般会在 LoadOperator 和 UpdateContextOperator 中使用。目前每个 storage 最多只能使用一个 sync push 的 PushOperator。

## PullOperator

在训练时，对于不存在的数据， PullOperator 需要将其初始化并存储到 storage 中，以保证 push 的梯度和参数值是对应的，此时 PullOperator 实际上包含了对 storage 的写操作。但是对于预估场景，可以仅返回一个默认值，此时 PullOperator 是只读的。 PullOperator 包含一个 _read_only 成员，用来指定这个 PullOperator 是否是只读的。

## ForEachOperator

遍历 storage 中的所有数据，可以返回一个统计结果。

## EraseIfOperator

继承 ForEachOperator ，删除满足条件的数据。

## DumpOperator

继承 ForEachOperator ， dump 具有参数 uri 和 file_number ，分别对应 dump 目录和每个节点的文件数量。 client 端会首先创建 uri 目录，将每个 shard 分配给为一个节点中的一个文件，然后每个文件发送一个请求，server 端收到请求后将相应 shard 的内容 dump 到这个文件中。

## LoadOperator

load 具有参数 path, server_concurency 和 need_rehash 。client 会根据 server_concurency 控制 load 的并发度。如果 path 是 hdfs 路径， client 会列出 path 目录所有文件，然后将文件路径发给不同的 server ，再由 server 实际读取数据。如果 path 是本地路径，client 会直接把 path 发给所有 server，然后 server 会直接读取自己 path 目录下的所有文件。通常情况下 need_rehash == true, 表示 load 时的 context 与 dump 时的 context 不同，这种情况下从文件中读取的数据会通过 push 操作重新分配 shard_id 并 push 到对应的 server 上。 当 need_rehash == false 时，要求 load 时 context 与 dump 时相同，且 path 是本地路径，因此从文件中读取的数据可以直接存储到当前 server 的 storage 中。

## UDFOperator

UDFOperator 可是实现包括 pull, push, for each, load, dump 在内的所有功能，并且可以灵活地定制返回 response 的时机，这对于一些异步优化是有用的。

## RestoreOperator

RestoreOperator 没有对应的 Handler ，由 server 自动调用，当一个 server 节点挂掉时如果重启这个 server 就会调用 RestoreOperator 。 RestoreOperator 可以定义两种恢复方式，一种是从文件中 restore，另一种是通过其他 server restore。从文件中 restore 一般需要遍历所有文件，但是只保留属于当前节点的数据。如果通过其他 server restore 则会向包含相同 shard 副本的其他 server 发送请求，流式获取数据。支持 restore 的 StorageOperator 中会包含一个 RestoreOperator。

## UpdateContextOperator

UpdateContextOperator 定义了如何 shuffle 数据，包括遍历 storage 把数据流式发送给目标 server ，以及另一个 server 如何接收这些数据。 UpdateContextOperator 还定义了在 update context 后如何将不属于当前 server 的数据删除。

## SyncOperator

SyncOperator 用于把 increment storage 中的训练增量同步给处于不同 rpc 空间的另一组 server 。其中有类似 DirectPushOperator 的部分，另外还包括遍历 increment storage 的方法。