package sparkCore.baseOpt.transform.keyValue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * aggregateByKey可分别定义分区内核分区间的计算规则
  * 取出每个分区相同key对应值的最大值，然后相加。
  */
object Opt_aggregateByKey {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 2), ("a", 6), ("b", 3), ("b", 8), ("c", 1), ("d", 9)), 4)
    println("分区前：" + rdd.partitions.size)
    //shuffle操作
    //参数1 初始值 参数2 分区内计算规则 参数3 分区间计算规则
    val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(0)(Math.max(_, _), _ + _)
    rdd2.glom().foreach(array => {
      println("aggregateByKey 后：" + array.mkString(","))
    })
    //结束任务
    sc.stop()
  }
}
