package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_FileDataSource {
  def main(args: Array[String]): Unit = {
    //获取StreamingContext
    val conf = new SparkConf().setAppName("StreamingWC").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置检查点
    ssc.checkpoint("file:/D:/data/checkpoint")
    //从指定文件夹中采集数据
    val fileDStream: DStream[String] = ssc.textFileStream("test")
    //扁平化，转换tuple
    val mapDStream: DStream[(String, Int)] = fileDStream.flatMap(_.split(" ")).map((_, 1))
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
