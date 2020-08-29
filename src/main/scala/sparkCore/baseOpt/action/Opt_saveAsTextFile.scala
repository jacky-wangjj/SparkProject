package sparkCore.baseOpt.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_saveAsTextFile {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 3), ("c", 3), ("d", 5)))

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")

    //结束任务
    sc.stop()
  }
}
