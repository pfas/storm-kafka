# 整体结构

## 目录结构

```
-src
	-java
		- config 配置的读取工具类
		- code 分为4个Bolt
		- msg 消息类型
		- spout 数据喷头
		- util 工具类
	-resources
		-application.yml 配置文件

-test
	- TestFeatureCompute 测试特征的计算（Mean, Std等这些特征）
	- TestInput  测试数据的输入（映射为POJO）
```

## 整体架构

**WindowBolt**: 接受原始的数据。维护一个队列，队列长度固定，每次将最新的K个数据分发给下层应用。

**FeatureComputeBolt**: 进行数据的处理。

**ModelBolt**: 请求模型

**ControlBolt**: 反向控制



## 注意问题

1. 怎么判断一条数据为该批次的第一条数据：

   累计流量为0，或者之前没有其他批次数据，或者之前批次和当前批次不一样

2. 特征处理后矩阵是怎么分布的：

   先进行Split，对于每一个Split进行计算。但是模型需要的特征是按照（均值，方差……） 这样的顺序进行排列的，所有这里还需要进行Merge。把按照Split进行排序变为按照统计量进行排序。

3. 数据的持久化没有做！