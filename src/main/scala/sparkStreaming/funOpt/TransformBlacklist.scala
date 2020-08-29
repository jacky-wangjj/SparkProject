package sparkStreaming.funOpt

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * 基于transform的实时广告计费日志黑名单过滤
  * 过滤掉黑名单中用户点击广告的日志
  */
object TransformBlacklist {
  def main(args: Array[String]): Unit = {
    //获取StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("TransformBlacklist").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    //设置检查点
    ssc.checkpoint("file:/D:/data/checkpoint")
    //从指定端口中采集数据
    val adsClickLogDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
    //加载黑名单数据
    val blacklist: Array[(String, Boolean)] = Array(("tom", true))
    val blacklistRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(blacklist, 2)
    //转换log -> (user,log)
    //log格式为 date username
    val userAdsClickLogDStream: DStream[(String, String)] = adsClickLogDStream.map(adsClickLog => (adsClickLog.split(" ")(1), adsClickLog))
    val validAdsClickLogDStream: DStream[String] = userAdsClickLogDStream.transform(userAdsClickLogRDD => {
      //关联黑名单RDD和日志RDD，使用user关联
      val joinedRDD: RDD[(String, (String, Option[Boolean]))] = userAdsClickLogRDD.leftOuterJoin(blacklistRDD)
      //过滤掉黑名单_2是true的数据,保留使filter内表达式为true的值
      val filteredRDD: RDD[(String, (String, Option[Boolean]))] = joinedRDD.filter(tuple => {
        if (tuple._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
      val validAdsClickLogRDD: RDD[String] = filteredRDD.map(tuple => tuple._2._1)
      validAdsClickLogRDD
    })
    validAdsClickLogDStream.print()
    //启动采集器
    ssc.start()
    //Driver等待采集器的执行
    ssc.awaitTermination()
  }
}
