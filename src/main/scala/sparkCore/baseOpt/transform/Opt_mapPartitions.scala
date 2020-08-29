package sparkCore.baseOpt.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_mapPartitions {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //mapPartitions对RDD的所有分区进行遍历 里面map为scala运算非算子
    //只需要发送分区个数个datas.map(data => data * 2)至excutor
    //效率优于map算子，减少了发送到excutor执行交互次数，可能出现OOM
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {
      datas.map(data => data * 2)
    })

    mapPartitionsRDD.collect().foreach(println)
    //结束任务
    sc.stop()
  }
}
