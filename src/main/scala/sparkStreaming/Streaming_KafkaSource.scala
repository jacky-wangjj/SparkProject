package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_KafkaSource {
  def main(args: Array[String]): Unit = {
    //获取StreamingContext
    val conf = new SparkConf().setAppName("StreamingWC").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置检查点
    ssc.checkpoint("file:/D:/data/checkpoint")
    //创建kafka采集
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "localhost:2181",
      "group",
      Map("topic" -> 2) //topic及其对应的分区数
    )
    //kafka消息为K,V形式，这里使用V即 _._2
    val mapDStream: DStream[(String, Int)] = kafkaDStream.flatMap(_._2.split(" ")).map((_, 1))
    //聚合数据
    val res: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)
    //打印结果
    res.print()
    //启动采集器
    ssc.start()
    //Driver等待采集器的执行
    ssc.awaitTermination()
  }
}
