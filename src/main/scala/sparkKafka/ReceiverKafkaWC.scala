package sparkKafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * 基于Receiver方式存在的问题：
  * 启用WAL机制，每次处理之前需要将该batch内的数据备份到checkpoint目录中，这降低了数据处理效率，同时加重了Receiver的压力；另外由于数据备份机制，会收到负载影响，负载一高就会出现延迟的风险，导致应用崩溃。
  * 采用MEMORY_AND_DISK_SER降低对内存的要求，但是在一定程度上影响了计算的速度。
  * 单Receiver内存。由于Receiver是属于Executor的一部分，为了提高吞吐量，提高Receiver的内存。但是在每次batch计算中，参与计算的batch并不会使用这么多内存，导致资源严重浪费。
  * 提高并行度，采用多个Receiver来保存kafka的数据。Receiver读取数据是异步的，不会参与计算。如果提高了并行度来平衡吞吐量很不划算。
  * Receiver和计算的Executor是异步的，在遇到网络等因素时，会导致计算出现延迟，计算队列一直在增加，而Receiver一直在接收数据，这非常容易导致程序崩溃。
  * 在程序失败恢复时，有可能出现数据部分落地，但是程序失败，未更新offsets的情况，这会导致数据重复消费。
  */
object ReceiverKafkaWC {

  def processRdd(linerdd: InputDStream[(String, String)]): Unit = {
    //提取K V中的V统计单词数量
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
    val brokerList: String = "127.0.0.1:9092"
    val groupId: String = "group1"
    val topic: String = "test"
    //配置信息类
    //启用Spark Streaming的预写日志机制（Write Ahead Log，WAL）
    val conf: SparkConf = new SparkConf()
      .setAppName("ReceiverKafkaWC")
      .setMaster("local[*]")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    //上下文对象
    val batchInterval: Duration = Seconds(5)
    val ssc: StreamingContext = new StreamingContext(conf, batchInterval)
    //设置WAL路径
    ssc.checkpoint("file:/D:/data/receiverdata")
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupId,
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )
    //创建一个Map,里面放入你要读取的Topic及消费topic的线程数
    val numTheads: Int = 1
    val topicMap: Map[String, Int] = Map[String, Int](topic -> numTheads)
    val stream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topicMap,
      StorageLevel.MEMORY_AND_DISK_SER
    )
    processRdd(stream)
    ssc.start()
    ssc.awaitTermination()
  }
}
