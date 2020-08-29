package sparkKafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.Cell
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

import scala.collection.mutable

/**
  * https://blog.csdn.net/a805814077/article/details/106531020
  */
object Kafka_DirectWithHBase {
  //保存偏移量
  def storeOffsets(offsetRanges: Array[OffsetRange], group: String): Unit = {
    val hbaseUtils: HBaseUtils = HBaseUtils.getHBaseUtils
    val tableName = "hadoop-topic-offset"
    val cf = "cf"
    for (offsetRange <- offsetRanges) {
      //topic-group作为rowkey
      val rk = s"${offsetRange.topic}-${group}"
      //partition号作为qualifier
      val partition = offsetRange.partition
      //偏移量offset作为value
      val offset = offsetRange.untilOffset
      //将结果保存到Hbase中
      hbaseUtils.insertOneColumn(tableName, rk, cf, (partition + ""), (offset + ""))
    }
    hbaseUtils.closeConn()
  }

  def createMsg(ssc: StreamingContext, kafkaParams: Map[String, String], topics: Set[String]): InputDStream[(String, String)] = {
    //从zookeeper中读取偏移量
    val offsets = getOffsets(topics, kafkaParams("group.id"))
    var messages: InputDStream[(String, String)] = null
    if (offsets.isEmpty) { //如果为空就从0开始读取
      messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        kafkaParams,
        topics
      )
    } else { //有值就从指定的offset位置开始读取
      val messageHandler = (msg: MessageAndMetadata[String, String]) => (msg.key(), msg.message())
      messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc,
        kafkaParams,
        offsets,
        messageHandler
      )
    }
    messages
  }

  /**
    * create 'hadoop-topic-offset', 'cf'
    * rowkey: topic-group
    *
    * @return offsets
    */
  def getOffsets(topics: Set[String], group: String): Map[TopicAndPartition, Long] = {
    val hbaseUtils: HBaseUtils = HBaseUtils.getHBaseUtils
    val offsets: mutable.Map[TopicAndPartition, Long] = mutable.Map[TopicAndPartition, Long]()
    //拿到HBase中的表名
    val tableName = "hadoop-topic-offset"
    //数据库中的列族名
    val cf = "cf"
    for (topic <- topics) {
      //行键
      val rk = s"${topic}-${group}"
      //获取分区与偏移量信息
      val partition2Offsets: Array[(Int, Long)] = hbaseUtils
        .getOneRow(tableName, rk, cf)
        .rawCells()
        .map(cell => {
          val qArr: Array[Byte] = cell.getQualifierArray
          val qLen: Int = cell.getQualifierLength
          val qOffset: Int = cell.getQualifierOffset
          val vArr: Array[Byte] = cell.getValueArray
          val vLen: Int = cell.getValueLength
          val vOffset: Int = cell.getValueOffset
          val partition: Int = new String(qArr, qOffset, qLen).toInt
          val value: Long = new String(vArr, vOffset, vLen).toLong
          (partition, value)
        })
      partition2Offsets.foreach { case (partition, offset) => {
        offsets.put(TopicAndPartition(topic, partition), offset)
      }
      }
    }
    hbaseUtils.closeConn()
    offsets.toMap
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
      .setAppName("Kafka_DirectWithHBase")
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
    val directKafkaStream: InputDStream[(String, String)] = createMsg(ssc, kafkaParams, topics)
    //处理逻辑
    processRdd(directKafkaStream)
    //更新偏移量
    storeOffsets(directKafkaStream.asInstanceOf[HasOffsetRanges].offsetRanges, kafkaParams("group.id"))
    ssc.start()
    ssc.awaitTermination()
    //    ssc.stop()
  }
}
