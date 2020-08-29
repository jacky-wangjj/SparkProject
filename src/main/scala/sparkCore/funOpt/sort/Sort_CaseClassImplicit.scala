package sparkCore.funOpt.sort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 使用隐式转换确定排序规则
  *
  * @author jacky-wangjj
  * @date 2020/8/29
  */
object Sort_CaseClassImplicit {
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
    //排序 只传入排序规则，不改变数据格式，只改变排序顺序
    //使用隐式转换，引入排序规则
    import StudentSortRules.OrderingStudentAge

    val sortedRDD: RDD[(String, Int, Double)] = studentsRDD.sortBy(stu => {
      Student4(stu._2, stu._3)
    })
    //打印排序结果
    sortedRDD.collect().foreach(println)
    //结束任务
    sc.stop()
  }
}

/**
  * 仅定义样例类，不需要继承Ordered类，重写compare排序方法了。
  * 排序规则在隐式转换中实现
  *
  * @param age
  * @param score
  */
case class Student4(age: Int, score: Double)
