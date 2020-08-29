package sparkCore.baseOpt.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_sortBy {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(2, 1, 4, 3, 2, 3, 5))
    //参数1 排序规则  参数2 true升序 参数3 结果分区数，默认与前一RDD分区数相同，分区内有序
    val sortByRDD: RDD[Int] = listRDD.sortBy(x => x, false)

    sortByRDD.collect().foreach(println)
    sortByRDD.glom().foreach(array => {
      println(array.mkString(","))
    })
    //结束任务
    sc.stop()
  }
}
