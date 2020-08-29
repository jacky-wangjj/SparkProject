package sparkCore.funOpt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Lineage_toDebugString {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "hbase"))

    val mapRDD: RDD[(String, Int)] = rdd.map((_, 1))

    val res: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    println("Lineage:\n" + res.toDebugString)
    println("map:窄依赖 \n" + mapRDD.dependencies)
    println("reduceByKey:宽依赖 \n" + res.dependencies)

    //结束任务
    sc.stop()
  }
}
