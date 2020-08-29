package sparkCore.funOpt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Task_Serializable {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "hbase"))

    //    val search: Search = new Search("h")
    val search: Search1 = new Search1("h")

    val matched: RDD[String] = search.getMatch(rdd)

    println(matched.collect().mkString(","))

    //结束任务
    sc.stop()
  }
}

/**
  * 序列化
  *
  * @param query
  */
class Search(query: String) extends Serializable {
  //过滤出包含query的数据 excutor中执行，Search.isMatch需传输给excutor
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含query的RDD
  def getMatch(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }
}

/**
  * 非序列化
  *
  * @param query
  */
class Search1(query: String) {
  def getMatch(rdd: RDD[String]): RDD[String] = {
    //driver中执行
    val q = query
    //excutor中执行，q需传输给excutor
    rdd.filter(x => x.contains(q))
  }
}
