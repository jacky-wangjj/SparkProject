package sparkCore.baseOpt.transform.keyValue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_mapValues {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 2), ("a", 6), ("b", 3), ("b", 8), ("c", 1), ("d", 9)), 4)
    println("分区前：" + rdd.partitions.size)
    //shuffle操作
    //针对（K,V）形式的类型只对V进行操作
    val rdd2: RDD[(String, Int)] = rdd.mapValues(_ + 1)
    println("rdd2：" + rdd2.collect().toBuffer)
    //结束任务
    sc.stop()
  }
}
