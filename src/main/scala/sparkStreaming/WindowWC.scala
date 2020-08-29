package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 窗口大小、滑动步长应该为采集周期的整数倍
  */
object WindowWC {
  def main(args: Array[String]): Unit = {
    //获取StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("StreamingWC").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    //设置检查点
    ssc.checkpoint("file:/D:/data/checkpoint")
    //从指定端口中采集数据
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
    //扁平化，转换tuple
    val mapDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" ")).map((_, 1))

    //调用窗口操作来计算数据的聚合。批次间隔5秒，窗口长度为15秒，滑动间隔为10秒
    val res: DStream[(String, Int)] = mapDStream.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(15), Seconds(10))
    res.print()
    //启动采集器
    ssc.start()
    //Driver等待采集器的执行
    ssc.awaitTermination()
  }
}
