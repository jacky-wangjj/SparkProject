package sparkCore.baseOpt.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_distinct {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 1, 3, 4, 3, 2, 5))
    //shuffle操作
    //参数 使用几个分区保留数据。
    val distinctRDD: RDD[Int] = listRDD.distinct(2)

    distinctRDD.collect().foreach(println)
    //结束任务
    sc.stop()
  }
}
