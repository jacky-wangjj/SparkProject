package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.scalatest.time.Seconds

object WindowWC {
  def main(args: Array[String]): Unit = {
    //获取StreamingContext
    val conf = new SparkConf().setAppName("StreamingWC").setMaster("local[2")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置检查点
    ssc.checkpoint("file:/D:/data/checkpoint")
    val dStream = ssc.socketTextStream("localhost", 8888)
    val tuples = dStream.flatMap(_.split(" ")).map((_, 1))

    //调用窗口操作来计算数据的聚合。批次间隔5秒，窗口长度为15秒，滑动间隔为10秒
    val res = tuples.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(15), Seconds(10))

    res.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
