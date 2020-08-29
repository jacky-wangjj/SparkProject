package sparkCore.baseOpt.transform.keyValue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_groupByKey {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (1, "ccc"), (4, "ddd")), 4)
    println("分区前：" + rdd.partitions.size)
    //shuffle操作
    val rdd2: RDD[(Int, Iterable[String])] = rdd.groupByKey(2)
    rdd2.glom().foreach(array => {
      println("groupByKey 后：" + array.mkString(","))
    })
    //结束任务
    sc.stop()
  }
}
