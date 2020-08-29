package sparkCore.baseOpt.transform.keyValue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_sortByKey {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 2), ("a", 1), ("a", 6), ("b", 3), ("b", 8), ("c", 1), ("d", 9)), 4)
    println("分区前：" + rdd.partitions.size)
    //shuffle操作
    //按key进行排序
    //参数1 是否升序  参数2 分区数
    val rdd2: RDD[(String, Int)] = rdd.sortByKey(true, 2)
    println("rdd2：" + rdd2.collect().toBuffer)
    //结束任务
    sc.stop()
  }
}
