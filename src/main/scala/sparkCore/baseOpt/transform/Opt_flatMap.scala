package sparkCore.baseOpt.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_flatMap {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val arrayListRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))
    //扁平化
    val flatMapRDD: RDD[Int] = arrayListRDD.flatMap(x => x)

    flatMapRDD.collect().foreach(println)
    //结束任务
    sc.stop()
  }
}
