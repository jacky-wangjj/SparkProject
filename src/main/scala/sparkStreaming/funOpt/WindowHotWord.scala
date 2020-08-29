package sparkStreaming.funOpt

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 基于滑动窗口的热点搜索词实时统计
  */
object WindowHotWord {
  def main(args: Array[String]): Unit = {
    //获取StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("WindowHotWord").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    //设置检查点
    ssc.checkpoint("file:/D:/data/checkpoint")
    //从指定端口中采集数据
    val searchLogsDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
    val searchWordsDStream: DStream[String] = searchLogsDStream.map(_.split(" ")(1))
    val searchWordPairsDStream: DStream[(String, Int)] = searchWordsDStream.map(searchWord => (searchWord, 1))
    val searchWordCountsDStream: DStream[(String, Int)] = searchWordPairsDStream.reduceByKeyAndWindow(
      (v1: Int, v2: Int) => v1 + v2,
      Seconds(60),
      Seconds(10)
    )
    //只是在transform内部提取搜索热度最高的3个词，并打印。返回值没有改变
    val finalDStream: DStream[(String, Int)] = searchWordCountsDStream.transform(searchWordCountsRDD => {
      //转换KV位置为VK
      val countSearchWordsRDD: RDD[(Int, String)] = searchWordCountsRDD.map(tuple => (tuple._2, tuple._1))
      //按key进行排序 降序
      val sortedCountSearchWordsRDD: RDD[(Int, String)] = countSearchWordsRDD.sortByKey(false)
      //转换VK位置为KV
      val sortedSearchWordCountsRDD: RDD[(String, Int)] = sortedCountSearchWordsRDD.map(tuple => (tuple._2, tuple._1))
      //提取前3个,且前面已经降序排序
      val top3SearchWordCounts: Array[(String, Int)] = sortedSearchWordCountsRDD.take(3)
      for (tuple <- top3SearchWordCounts) {
        println(tuple)
      }
      searchWordCountsRDD
    })
    finalDStream.print()

    //启动采集器
    ssc.start()
    //Driver等待采集器的执行
    ssc.awaitTermination()
  }
}
