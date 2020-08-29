package sparkKafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

import scala.collection.mutable
import scala.collection.JavaConversions._

object Kafka_DirectWithZookeeper {
  //保存偏移量
  def storeOffsets(offsetRanges: Array[OffsetRange], group: String, curator: CuratorFramework): Unit = {
    for (offsetRange <- offsetRanges) {
      val topic = offsetRange.topic
      val partition = offsetRange.partition
      val offset = offsetRange.untilOffset
      val path = s"${topic}/${group}/${partition}"
      checkExists(path, curator)
      curator.setData().forPath(path, new String(offset + "").getBytes())
    }
  }

  def createMsg(ssc: StreamingContext, kafkaParams: Map[String, String], topics: Set[String], curator: CuratorFramework): InputDStream[(String, String)] = {
    //从zookeeper中读取offset
    val offsets: Map[TopicAndPartition, Long] = getOffsets(topics, kafkaParams("group.id"), curator)
    var messages: InputDStream[(String, String)] = null
    if (offsets.isEmpty) { //为空则从0开始读取
      messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        kafkaParams,
        topics
      )
    } else { //有就从指定位置开始读取
      val messageHandler: MessageAndMetadata[String, String] => (String, String) = (msgH: MessageAndMetadata[String, String]) => (msgH.key(), msgH.message())
      messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc,
        kafkaParams,
        offsets,
        messageHandler
      )
    }
    messages
  }

  def getOffsets(topics: Set[String], group: String, curator: CuratorFramework): Map[TopicAndPartition, Long] = {
    val offsets = mutable.Map[TopicAndPartition, Long]()
    for (topic <- topics) {
      val parent = s"${topic}/${group}"
      checkExists(parent, curator)
      //此时目录一定存在
      for (partition <- curator.getChildren.forPath(parent)) {
        val path = s"${parent}/${partition}"
        val offset = new String(curator.getData.forPath(path)).toLong
        offsets.put(TopicAndPartition(topic, partition.toInt), offset)
      }
    }
    offsets.toMap
  }

  //检测是否存在
  def checkExists(path: String, curator: CuratorFramework): Unit = {
    if (curator.checkExists().forPath(path) == null) {
      //不存在则创建
      curator.create()
        .creatingParentsIfNeeded()
        .forPath(path)
    }
  }

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

  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf()
      .setAppName("Kafka_DirectWithZookeeper")
      .setMaster("local[*]")
    //是否启用SparkStreaming内部的backpressure机制
    //当batch processing time>batch interval这种情况持续过长的时间，会造成数据在内存中堆积，导致receiver所在Executor内存溢出等问题
    //      .set("spark.streaming.backpressure.enabled", "false")
    //设置spark程序每秒中从每个partition分区读取的最大的数据条数
    //      .set("spark.streaming.kafka.maxRatePerPartition", "100")
    val batchInterval = Seconds(5)

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
    //上下文对象
    val ssc: StreamingContext = new StreamingContext(conf, batchInterval)
    //获取kafka流数据
    val directKafkaStream: InputDStream[(String, String)] = createMsg(ssc, kafkaParams, topics, client)
    //处理逻辑
    processRdd(directKafkaStream)
    //更新偏移量
    storeOffsets(directKafkaStream.asInstanceOf[HasOffsetRanges].offsetRanges, kafkaParams("group.id"), client)
    ssc.start()
    ssc.awaitTermination()
    //    ssc.stop()
  }

  //构建一个Curator的Client
  val client = {
    val client: CuratorFramework = CuratorFrameworkFactory.builder()
      .connectString("localhost:2181")
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .namespace("kafka/consumers/offsets")
      .build()
    client.start()
    client
  }
}
