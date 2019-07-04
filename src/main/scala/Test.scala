/**
  * Created by wangjj17 on 2019/1/7.
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def test(): Unit = {
    val conf = new SparkConf().setAppName("mySpark")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    //生成RDD有两种方式：
    //1.sc.parallelize()
    //2.sc.textFile()
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9)).map(_ * 3)
    val filtered: Array[Int] = rdd.filter(_ > 10).collect()
    //对集合求和
    println(rdd.reduce(_ + _))
    //输出大于10的元素
    for (arg <- filtered) {
      print(arg + " ")
    }
    println()
  }

  def main(args: Array[String]) {
    println("hello world!")
    test()
  }
}
