package sparkCore.funOpt.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * groupByKey和map实现
  */
object SparkWC_groupByKey_map {
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
    //groupByKey将相同key的数据放入同一个Array中
    val group: RDD[(String, Iterable[Int])] = paired.groupByKey(2)
    //使用map对相同key的数据进行求和
    val reduced: RDD[(String, Int)] = group.map(x => (x._1, x._2.sum))
    val res: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //保存
    println(res.collect().toBuffer)
    //    res.saveAsTextFile(args(1))
    //结束任务
    sc.stop()
  }
}
