package sparkStreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * nc -l -p 8888
  */
object StreamingWC {
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
    //按批次累加需要调用updateStateByKey方法
    val res: DStream[(String, Int)] = mapDStream.updateStateByKey(func, new HashPartitioner(ssc.sparkContext.defaultMinPartitions), false)

    res.print()
    //启动采集器
    ssc.start()
    //Driver等待采集器的执行
    ssc.awaitTermination()
  }

  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(t => {
      (t._1, t._2.sum + t._3.getOrElse(0))
    })
  }
}
