package sparkCore.baseOpt.transform.keyValue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_combineByKey {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 2), ("a", 6), ("b", 3), ("b", 8), ("c", 1), ("d", 9)), 4)
    println("分区前：" + rdd.partitions.size)
    //shuffle操作
    //参数类似aggregateByKey，初始值定义为初始结构转换，
    // 例如：_转换成(_,1)
    //参数2的第一部分acc需写类型(Int, Int)，否则会出错，不能运行时推断类型
    val rdd2: RDD[(String, (Int, Int))] = rdd.combineByKey((_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    rdd2.glom().foreach(array => {
      println("combineByKey 后：" + array.mkString(","))
    })
    val rdd3: RDD[(String, Int)] = rdd2.map(x => (x._1, x._2._1 / x._2._2))
    println("map后：" + rdd3.collect().toBuffer)
    //结束任务
    sc.stop()
  }
}
