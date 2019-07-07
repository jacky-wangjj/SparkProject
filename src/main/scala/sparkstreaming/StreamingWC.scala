package sparkstreaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.StreamingContext
import org.scalatest.time.Seconds

object StreamingWC {
  def main(args: Array[String]): Unit = {
    //获取StreamingContext
    val conf = new SparkConf().setAppName("StreamingWC").setMaster("local[2")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置检查点
    ssc.checkpoint("file:/D:/data/checkpoint")
    val dStream = ssc.socketTextStream("localhost", 8888)
    val tuples = dStream.flatMap(_.split(" ")).map((_, 1))
    //按批次累加需要调用updateStateByKey方法
    val res = tuples.updateStateByKey(func, new HashPartitioner(ssc.sparkContext.defaultMinPartitions), false)

    res.print()

    ssc.start()

    ssc.awaitTermination()
  }

  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(t => {
      (t._1, t._2.sum + t._3.getOrElse(0))
    })
  }
}
