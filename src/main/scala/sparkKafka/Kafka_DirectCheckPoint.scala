package sparkKafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object Kafka_DirectCheckPoint {
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
      .setAppName("Kafka_DirectCheckPoint")
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
    val checkpointPath: String = "file:/D:/data/receiverdata"

    def creatingFunc(): StreamingContext = {
      //上下文对象
      val ssc: StreamingContext = new StreamingContext(conf, batchInterval)
      //使用checkpoint来存储offset信息
      ssc.checkpoint(checkpointPath)
      ssc
    }

    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointPath, creatingFunc)
    val directKafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics
    )
    processRdd(directKafkaStream)
    ssc.start()
    ssc.awaitTermination()
    //    ssc.stop()
  }
}
