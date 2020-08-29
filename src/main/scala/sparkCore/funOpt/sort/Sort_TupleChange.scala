package sparkCore.funOpt.sort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 可以在sortBy时改变元祖形态，生成新的元祖，利用新元祖的排序规则进行排序
  *
  * @author jacky-wangjj
  * @date 2020/8/29
  */
object Sort_TupleChange {
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
    //将需要排序的字段放到新的元组中，用作比较规则
    //先比较第一个数据，负号表示降序，再比较第二个数据
    //负号可以应用至Int Double，不可应用于String
    val sortedRDD: RDD[(String, Int, Double)] = studentsRDD.sortBy(stu => (-stu._3, stu._2))
    //打印排序结果
    sortedRDD.collect().foreach(println)
    //结束任务
    sc.stop()
  }
}
