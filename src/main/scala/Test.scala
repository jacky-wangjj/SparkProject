/**
  * Created by wangjj17 on 2019/1/7.
  */
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def test(): Unit = {
    val conf = new SparkConf().setAppName("mySpark")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9)).map(_*3)
    val mappedRDD = rdd.filter(_>10).collect()
    //对集合求和
    println(rdd.reduce(_+_))
    //输出大于10的元素
    for(arg <- mappedRDD) {
      print(arg+" ")
    }
    println()
  }
  def main(args: Array[String]) {
    println("hello world!")
    test()
  }
}
