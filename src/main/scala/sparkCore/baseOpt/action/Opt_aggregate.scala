package sparkCore.baseOpt.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 参数：(zeroValue: U)(seqOp: (U,T)=>U, combOp:(U,U)=>U)
  * aggregate函数将每个分区里面的元素通过seqOp和初始值进行聚合，
  * 然后用combine函数将每个分区的结果和初始值进行combine操作。
  * 最终返回的类型不需要和RDD中元素类型一致
  * 分区内，分区间均加初始值，与aggregateByKey不同
  */
object Opt_aggregate {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    val res: Int = listRDD.aggregate(0)(_ + _, _ + _)
    println(res)

    //结束任务
    sc.stop()
  }
}
