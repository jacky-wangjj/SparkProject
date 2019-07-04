import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWC {
  def main(args: Array[String]): Unit = {
    //配置program arguments D:\data\input\data.txt D:\data\output
    // hdfs://node01:8020/data/input

    //配置信息类
    val conf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")
    //上下文对象
    val sc = new SparkContext(conf)
    //读取数据
    val lines: RDD[String] = sc.textFile(args(0))
    //处理数据
    val words: RDD[String] = lines.flatMap(_.split("\t"))
    val paired: RDD[(String, Int)] = words.map((_, 1))
    val reduced: RDD[(String, Int)] = paired.reduceByKey(_ + _)
    val res: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //保存
    println(res.collect().toBuffer)
    res.saveAsTextFile(args(1))
    //结束任务
    sc.stop()
  }
}
