package sparkCore.baseOpt.transform.twoValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Opt_cartesian {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(1 to 5)
    val rdd2: RDD[Int] = sc.parallelize(4 to 10)
    //笛卡尔集
    val rdd3: RDD[(Int, Int)] = rdd1.cartesian(rdd2)

    rdd3.glom().foreach(array => {
      println(array.mkString(","))
    })
    println("分区数：" + rdd3.partitions.size)
    println(rdd3.collect().mkString(","))

    //结束任务
    sc.stop()
  }
}
