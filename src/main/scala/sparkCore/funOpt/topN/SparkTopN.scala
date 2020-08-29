package sparkCore.funOpt.topN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
;

object SparkTopN {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf = new SparkConf().setAppName("SparkGroupTopN").setMaster("local[1]")
    //上下文对象
    val sc = new SparkContext(conf)
    //top N
    val n = 2
    //读取数据
    val lines: RDD[String] = sc.textFile("D:\\ML\\Code\\SparkProject\\src\\main\\scala\\sparktopn\\data.txt")
    //切分数据(className,score)
    val cs = lines.map(line => {
      val info = line.split(" ")
      val className = info(0)
      val score = info(1).toInt
      (className, score)
    })
    //通过groupByKey算子对class进行分组/排序，取topN
    val result = cs.groupByKey().map(tuple => {
      val className = tuple._1
      val topn = tuple._2.toArray.sortWith(_ > _).take(n)
      (className, topn)
    })
    //打印结果
    result.foreach(item => {
      val className = item._1
      println("Class: " + className + " top " + n + " is: ")
      item._2.foreach(score => {
        println(score)
      })
    })
  }
}
