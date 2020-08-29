package sparkCore.baseOpt.transform.keyValue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_join {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 2), ("b", 3), ("c", 1), ("d", 9)), 4)
    val rdd2: RDD[(String, Int)] = sc.parallelize(Array(("a", 4), ("b", 8), ("c", 3)), 4)
    //shuffle操作
    //针对（K,V）形式的类型对相同的key进行连接
    //只保留key相同的数据，相当于inner join
    val rdd3: RDD[(String, (Int, Int))] = rdd2.join(rdd1)
    println("rdd3：" + rdd3.collect().toBuffer)
    //结束任务
    sc.stop()
  }
}
