# 说明



- 如何启动

  > Cep.testDemo

- 没有配置 `checkpoint`

  > 实际生产需要 ck

- 数据源

> 自定 source ，mock 数据流

## 设计思路

## 规则的维护

> reference :
>
> https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/broadcast_state/

> 注意：
>
> 1. 规则流入必须以并行度为1的形式，保障读取顺序性（本`demo`）。多并行度需要时间戳类似的东西确保有序性。给的`Rule`没有能够作为版本标识的东西。
>
> 2. 利用`BroadcastState` 作为容错。
> 3. 

1. `Ruleid` 作为规则的唯一标识
2. 只执`DELETE`，`PAUSE`, `ACTIVE`，`UPDATE`行为
3. `update` 提供的 `null` 值不覆盖旧值，非`null`值覆盖。

## 窗口

> reference :
>
> https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/windows/

> 注意：
>
> 1. 该`demo`实现的是一个`process time`触发(因此不考虑`watermark`，`timestamp`)，`process timer`按照`FIRE_LAG` 定期触发输出的窗口。（**正常来说，ontimer 必须判定一下触发的是 process time 还是 event time，demo，别注意细节**）
>
> 2. 为了依赖`checkpoint` 保证 `state` (窗口的底层)的安全，必须使用`keyed stream`。否则只能是`operator state`(source 才能用该state，要么就是全局窗口，牺牲并发和性能)。依赖`keyed stream` 自动解决并行度修改的`state` `rehash`问题。
>
> 3. 由于`Metric` 的操作都是可以预处理(`预聚合`)，按照使用的最小查询粒度，保存数据到`state`，而不是原始数据。(类型 flink sql  新版处理`Sliding windows`不再放入多个窗口，而是基于滑动的粒度组建窗口)



## 反馈的问题

### 内存`leak`

> 结论，暂时没发现

1. `leak` 的定义是有内存多次`gc`后无法回收(非常量，已过期变量)，比如`map`中的`key`改变了，`classloader`溢出等。
2. visualvm 看`Gc` 执行图，一个小时，没有不可回收缓慢增长的趋势。(测试数据已经改为`eventtime`不断自增) 

> timer 不会触发多个，只会按照`FIRE_LAG` 发送，唯一可能 `leak` 就是 `map` 对象。

1. 不存在一堆`abcxxx`， `abcxxx2` 这种对象。只可能存在容器的泄露，这个一般需要长期监控。线上`headdump on xxx` 分析解决一波就over了，跑了一个小时都没触发。
2. `mat`分析`source`下list就是常量长期，不回收也正常

## 广播的问题

1. 不明白哪里有问题



