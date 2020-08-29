package sparkCore.baseOpt.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_reduce {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //求和
    val res: Int = listRDD.reduce(_ + _)

    println(res)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 3), ("c", 3), ("d", 5)))
    //(K,V)类型数据处理
    val tuple: (String, Int) = rdd1.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    println(tuple)

    //结束任务
    sc.stop()
  }
}
