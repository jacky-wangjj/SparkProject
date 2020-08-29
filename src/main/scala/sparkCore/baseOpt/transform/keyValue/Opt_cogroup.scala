package sparkCore.baseOpt.transform.keyValue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_cogroup {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 2), ("b", 3), ("c", 1), ("d", 9)), 4)
    val rdd2: RDD[(String, Int)] = sc.parallelize(Array(("a", 4), ("b", 8), ("c", 3)), 4)
    //shuffle操作
    //在类型（K,V）和（K,W）的RDD调用，返回一个（K,(Iterale<V>,Iterale<W>))的RDD
    //结果相当于outer join
    val rdd3: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd2.cogroup(rdd1)
    println("rdd3：" + rdd3.collect().toBuffer)
    //结束任务
    sc.stop()
  }
}
