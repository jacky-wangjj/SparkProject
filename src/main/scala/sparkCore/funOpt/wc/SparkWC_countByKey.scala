package sparkCore.funOpt.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 使用countByKey，结果为map
  */
object SparkWC_countByKey {
  def main(args: Array[String]): Unit = {
    //配置program arguments D:\data\input\data.txt D:\data\output
    // hdfs://node01:8020/data/input

    //配置信息类
    val conf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")
    //上下文对象
    val sc = new SparkContext(conf)
    //读取数据
    //    val lines: RDD[String] = sc.textFile(args(0))
    //处理数据
    //    val words: RDD[String] = lines.flatMap(_.split("\t"))
    val words: RDD[String] = sc.makeRDD(Array("one", "two", "three", "four", "one", "two", "three", "two", "three"))
    val paired: RDD[(String, Int)] = words.map((_, 1))
    val res: collection.Map[String, Long] = paired.countByKey()
    //保存
    println(res.mkString(","))
    //    res.saveAsTextFile(args(1))
    //结束任务
    sc.stop()
  }
}
