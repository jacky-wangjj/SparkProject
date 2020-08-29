package sparkCore.baseOpt.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 保留使filter内表达式为true的值
  */
object Opt_filter {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //传入boolean返回值的表达式，保留使filter内表达式为true的值
    val filterRDD: RDD[Int] = listRDD.filter(x => x % 2 == 0)

    filterRDD.collect().foreach(println)
    //结束任务
    sc.stop()
  }
}
