package sparkCore.funOpt.partitionMax

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object partitionMax {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)
    //将同一个分区中的数据放入一个Array中
    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.collect().foreach(array => {
      var max = Int.MinValue
      array.foreach(x => {
        max = Math.max(x, max)
      })
      println(array.mkString(",") + " Max:" + max)
    })

    //结束任务
    sc.stop()
  }
}
