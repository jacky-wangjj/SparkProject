package sparkCore.baseOpt.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每个key对应的元素个数
  */
object Opt_countByKey {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 3), ("c", 3), ("d", 5)))

    val res: collection.Map[String, Long] = rdd.countByKey()
    println(res.mkString(","))

    //结束任务
    sc.stop()
  }
}
