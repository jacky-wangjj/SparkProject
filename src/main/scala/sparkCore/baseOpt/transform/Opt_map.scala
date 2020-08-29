package sparkCore.baseOpt.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Opt_map {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //发送x * 2至excutor
    val mapRDD: RDD[Int] = listRDD.map(x => x * 2)

    mapRDD.collect().foreach(println)
    //结束任务
    sc.stop()
  }
}
