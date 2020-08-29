package sparkCore.baseOpt.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Opt_sample {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //参数1：是否放回  参数2：抽样比例  参数3：种子 相同种子两次抽取结果相同，生产使用System.currentTimeMillis()生产随机值
    val sampleRDD: RDD[Int] = listRDD.sample(false, 0.2, System.currentTimeMillis())

    sampleRDD.collect().foreach(println)
    //结束任务
    sc.stop()
  }
}
