package sparkCore.baseOpt.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_Coalesce {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 1, 3, 4, 3, 2, 5))
    println("缩减分区前：" + listRDD.partitions.size)
    listRDD.glom().foreach(array => {
      println(array.mkString(","))
    })

    //非shuffle操作，简单的合并分区
    val coalesceRDD: RDD[Int] = listRDD.coalesce(2)
    println("缩减分区后：" + coalesceRDD.partitions.size)
    coalesceRDD.glom().foreach(array => {
      println(array.mkString(","))
    })
    //结束任务
    sc.stop()
  }
}
