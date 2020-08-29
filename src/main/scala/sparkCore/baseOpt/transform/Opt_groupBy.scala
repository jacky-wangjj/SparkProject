package sparkCore.baseOpt.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_groupBy {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //返回结果是一个元组
    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(x => x % 2)

    groupByRDD.collect().foreach(println)
    //结束任务
    sc.stop()
  }
}
