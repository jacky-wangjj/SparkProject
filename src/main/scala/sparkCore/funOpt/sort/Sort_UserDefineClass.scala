package sparkCore.funOpt.sort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 学生数据包含三个字段（姓名，年龄，成绩），
  * 需要按成绩进行降序排序，成绩相同的按照年龄进行升序排序
  *
  * @author jacky-wangjj
  * @date 2020/8/29
  */
object Sort_UserDefineClass {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sort func")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val linesRDD: RDD[String] = sc.parallelize(Array("张三 16 98.3", "李四 14 98.3", "王五 34 100.0", "赵六 26 98.2", "田七 18 98.2", "陈九 18 60.3"), 4)

    val studentsRDD: RDD[Student] = linesRDD.map(line => {
      val fields: Array[String] = line.split(" ")
      new Student(fields(0), fields(1).toInt, fields(2).toDouble)
    })
    //排序规则是Student类中重写的compare方法
    val sortedRDD: RDD[Student] = studentsRDD.sortBy(stu => stu)
    //打印排序结果
    sortedRDD.collect().foreach(println)
    //结束任务
    sc.stop()
  }
}

/**
  * 自定义Student类，重写排序方法，继承Ordered类，重写compare方法
  * 还需要继承Serializable类，序列化自定义类
  *
  * @param name
  * @param age
  * @param score
  */
class Student(val name: String, val age: Int, val score: Double) extends Ordered[Student] with Serializable {
  /**
    * 自定义排序规则：按照score进行降序排序，若score一样比较age
    *
    * @param that
    * @return
    */
  override def compare(that: Student): Int = {
    if (this.score == that.score) {
      this.age - that.age
    } else if (this.score < that.score) {
      1
    } else {
      -1
    }
  }

  override def toString: String = s"name: $name, age: $age, score: $score"
}
