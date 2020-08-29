package sparkKafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
  * Direct方式可保证Exactly-once
  * 基于Direct方式的优势：
  * 简化并行读取：如果要读取多个partition，不需要创建多个输入DStream然后对他们进行union操作。Spark会创建跟Kafka partition一样多的RDD partition，并且会并行从kafka中读取数据。所以在kafka partition和RDD partition之间，有一一对应的关系。
  * 高性能：如果要保证数据零丢失，在基于Receiver的方式中，需要开启WAL机制。这种方式其实效率很低，因为数据实际被复制了两份，kafka自己本身就有高可靠的机制，会对数据复制一份，而这里又会复制一份到WAL中。而基于Direct的方式，不依赖于Receiver，不需要开启WAL机制，只要kafka中做了数据的复制，那么就可以通过kafka的副本进行恢复。
  * 强一致语义：基于Receiver的方式，使用kafka的高阶API来在Zookeeper中保存消费过的offset。这是消费kafka数据的传统方式。这种方式配合WAL机制，可以保证数据零丢失的高可靠性，但是却无法保证数据被处理一次且仅一次，可能会处理两次。因为Spark和Zookeeper之间可能是不同步的。基于Direct的方式，使用kafka的简单api，Spark Streaming自己就负责追踪消费的offset，并保存在checkpoint中。Spark自己一定是同步的，因此可以保证数据时消费一次且仅消费一次。
  * 降低资源：Direct不需要Receiver，其申请的Executors全部参与到计算任务中；而Receiver则需要专门的Receivers来读取kafka数据且不参与计算。因此相同的资源申请，Direct能够支持更大的业务。Receiver与其他Executor是异步的，并持续不断接收数据，对于小业务量的场景还好，如果遇到大业务量时，需要提高Receiver的内存，但是参与计算的Executor并不需要那么多的内存，而Direct因为没有Receiver，而是在计算的时候读取数据，然后直接计算，所以对内存的要求很低。
  * 鲁棒性更好：基于Receiver方式需要Receiver来异步持续不断的读取数据，因此遇到网络、存储负载等因素，导致实时任务出现堆积，但Receiver却还在持续读取数据，此种情况容易导致计算崩溃。Direct则没有这种顾虑，其Driver在触发batch计算任务时，才会读取数据并计算，队列出现堆积并不不会引起程序的失败。
  * 基于Direct方式的不足
  * Direct方式需要采用checkpoint或者第三方存储来维护offset，而不是像Receiver那样，通过Zookeeper来维护offsets，提高了用户的开发成本。
  * 基于Receiver方式指定topic指定consumer的消费情况均能够通过Zookeeper来监控，而Direct则没有这么便利，如果想做监控并可视化，则需要投入人力开发。
  */
object DirectKafkaWC {

  def processRdd(linerdd: InputDStream[(String, String)]): Unit = {
    val wordrdd: DStream[String] = linerdd.flatMap {
      _._2.split(" ")
    }
    //    val resultrdd: DStream[(String, Long)] = wordrdd.map(x => (x, 1L)).reduceByKey(_ + _)
    val resultrdd: DStream[(String, Long)] = wordrdd.map(x => (x, 1L))
      .updateStateByKey[Long]((x: Seq[Long], y: Option[Long]) => {
      Some(x.sum + y.getOrElse(0L))
    })
    resultrdd.print()
  }

  def saveOffsetInZk(linerdd: InputDStream[(String, String)], zkClient: ZkClient, topicDirs: ZKGroupTopicDirs): Unit = {
    //用于保存当前offset范围
    var offsetRanges: Array[OffsetRange] = Array.empty
    //获取当前rdd数据对应的offset
    offsetRanges = linerdd.asInstanceOf[HasOffsetRanges].offsetRanges
    for (o <- offsetRanges) {
      val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
      ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)
    }
    //    linerdd.foreachRDD(rdd => {
    //    for (o <- offsetRanges) {
    //      val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
    //      ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)
    //    }
    //      rdd.foreach(println)
    //    })
  }

  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf()
      .setAppName("DirectKafkaWC")
      .setMaster("local[*]")
    //是否启用SparkStreaming内部的backpressure机制
    //当batch processing time>batch interval这种情况持续过长的时间，会造成数据在内存中堆积，导致receiver所在Executor内存溢出等问题
    //      .set("spark.streaming.backpressure.enabled", "false")
    //设置spark程序每秒中从每个partition分区读取的最大的数据条数
    //      .set("spark.streaming.kafka.maxRatePerPartition", "100")
    //上下文对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val topic: String = "test"
    val customGroup: String = "testGroup"
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> "127.0.0.1:9092",
      "group.id" -> customGroup,
      "auto.offset.reset" -> "smallest",
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )
    //创建一个set,里面放入你要读取的Topic
    val topics: Set[String] = Set[String](topic)
    val zkClient: ZkClient = new ZkClient(
      "120.0.0.1:2181",
      Integer.MAX_VALUE,
      10000,
      ZKStringSerializer
    )
    val topicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(customGroup, topic)
    //获取kafka对应topic的分区数
    val children: Int = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    //设置第一批数据读取的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    var directKafkaStream: InputDStream[(String, String)] = null
    //如果zk下有该消费者的offset信息，则从zk下保存的offset位置开始读取，否则从最新的数据开始读取（受auto.offset.reset设置影响，此处默认）
    if (children > 0) {
      //将zk下保存的该主题该消费者的每个分区的offset值添加到fromOffsets中
      for (i <- 0 until children) {
        //获取对应kafka中topic分区i的offset
        val partitionOffset: String = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/$i")
        val tp: TopicAndPartition = TopicAndPartition(topic, i)
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        fromOffsets += (tp -> partitionOffset.toLong)
        println("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
        val messageHandler: MessageAndMetadata[String, String] => (String, String) = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
        directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
          ssc,
          kafkaParams,
          fromOffsets,
          messageHandler
        )
      }
    } else {
      val directKafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        kafkaParams,
        topics
      )
    }
    processRdd(directKafkaStream)
    //offset值写会到zk上
    saveOffsetInZk(directKafkaStream, zkClient, topicDirs)
    ssc.start()
    ssc.awaitTermination()
    //    ssc.stop()
  }
}
