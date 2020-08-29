package sparkStreaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * transform允许DStream上执行任意的RDD-TO-RDD函数
  */
object Window_transform {
  def main(args: Array[String]): Unit = {
    //获取StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("StreamingWC").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    //设置检查点
    ssc.checkpoint("file:/D:/data/checkpoint")
    //从指定端口中采集数据
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)

    //transform允许DStream上执行任意的RDD-TO-RDD函数
    val res: DStream[((String, String), Int)] = socketDStream.transform(rdd => {
      //执行位置Driver 周期执行
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
      val ts: String = sdf.format(System.currentTimeMillis())
      //执行位置Excutor
      val result: RDD[((String, String), Int)] = rdd.flatMap(_.split(" "))
        .filter(word => word.nonEmpty)
        .map(word => ((word, ts), 1))
        .reduceByKey(_ + _)
        .sortBy(t => t._2, false)
      result
    })
    res.print()

    //设置窗口大小。批次间隔5秒，窗口长度为15秒，滑动间隔为10秒
    //    val windowDStream: DStream[String] = socketDStream.window(Seconds(15), Seconds(10))
    //遍历window中的rdd
    //    windowDStream.foreachRDD(rdd => println(rdd))
    //启动采集器
    ssc.start()
    //Driver等待采集器的执行
    ssc.awaitTermination()
  }
}
