package sparkCore.funOpt.sort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author jacky-wangjj
  * @date 2020/8/29
  */
object Sort_UserDefineClass2 {
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
    val sortedRDD: RDD[(String, Int, Double)] = studentsRDD.sortBy(stu => {
      new Student2(stu._2, stu._3)
    })
    //打印排序结果
    sortedRDD.collect().foreach(println)
    //结束任务
    sc.stop()
  }
}

/**
  * 自定义Student类，重写排序方法，继承Ordered类，重写compare方法
  * 还需要继承Serializable类，序列化自定义类
  * 该类中定义的是排序规则，与排序无关的属性可以不出现
  *
  * @param age
  * @param score
  */
class Student2(val age: Int, val score: Double) extends Ordered[Student2] with Serializable {
  /**
    * 自定义排序规则：按照score进行降序排序，若score一样比较age
    *
    * @param that
    * @return
    */
  override def compare(that: Student2): Int = {
    if (this.score == that.score) {
      this.age - that.age
    } else if (this.score < that.score) {
      1
    } else {
      -1
    }
  }
}
