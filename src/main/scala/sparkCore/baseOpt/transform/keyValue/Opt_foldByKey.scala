package sparkCore.baseOpt.transform.keyValue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_foldByKey {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 2), ("a", 6), ("b", 3), ("b", 8), ("c", 1), ("d", 9)), 4)
    println("分区前：" + rdd.partitions.size)
    //shuffle操作
    //aggregateByKey的简化操作，分区内核分区间计算规则相同
    val rdd2: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
    rdd2.glom().foreach(array => {
      println("aggregateByKey 后：" + array.mkString(","))
    })
    //结束任务
    sc.stop()
  }
}
