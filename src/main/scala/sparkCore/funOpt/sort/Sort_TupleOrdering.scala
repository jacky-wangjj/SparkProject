package sparkCore.funOpt.sort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 不改变元组的形态，使用Ordering中的on方法改变元组比较的规则
  *
  * @author jacky-wangjj
  * @date 2020/8/29
  */
object Sort_TupleOrdering {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sort func")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val linesRDD: RDD[String] = sc.parallelize(Array("张三 16 98.3", "李四 14 98.3", "王五 34 100.0", "赵六 26 98.2", "田七 18 98.2", "陈九 18 60.3"), 4)

    val studentsRDD: RDD[(String, Int, Double)] = linesRDD.map(line => {
      val fields: Array[String] = line.split(" ")
      (fields(0), fields(1).toInt, fields(2).toDouble)
    })
    //以隐式转换的方式，通过执行元组的排序规则来进行排序
    //Ordering[(Double, Int)]元组比较的样式格式
    //[(String, Int, Double)]原始元组样式格式
    //(t => (-t._3, t._2)) 元组内数据的比较规则，先比较第一个数据，负号表示降序，再比较第二个数据
    //负号可以应用至Int Double，不可应用于String
    implicit val rules = Ordering[(Double, Int)].on[(String, Int, Double)](t => (-t._3, t._2))
    val sortedRDD: RDD[(String, Int, Double)] = studentsRDD.sortBy(stu => stu)
    //打印排序结果
    sortedRDD.collect().foreach(println)
    //结束任务
    sc.stop()
  }
}
