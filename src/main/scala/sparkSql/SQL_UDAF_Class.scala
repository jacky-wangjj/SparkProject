package sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object SQL_UDAF_Class {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL_DataFrame")
    //设置spark.sql.warehouse.dir的路径
    conf.set("spark.sql.warehouse.dir", "file:/D:/ML/Code/SparkProject/src/main/scala/sparksql/schema")
    //创建SparkSession
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //使用rdd.toDF需先引入隐式转换规则
    import sparkSession.implicits._
    //创建自定义聚合函数
    val udaf_class: MyAgeAvgClassFunction = new MyAgeAvgClassFunction
    //将聚合函数转换为查询列
    val avgCol: TypedColumn[UserBean, Double] = udaf_class.toColumn.name("avgAge")
    //创建RDD
    val rdd: RDD[(Int, String, Int)] = sparkSession.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    //rdd转换为DataFrame
    val df: DataFrame = rdd.toDF("id", "name", "age")
    val userDS: Dataset[UserBean] = df.as[UserBean]
    //应用函数
    userDS.select(avgCol).show()
    //释放资源
    sparkSession.stop()
  }
}

case class UserBean(id: Int, name: String, age: Int)

case class AvgBuffer(var sum: Int, var count: Int)

/**
  * 声明用户自定义聚合函数 强类型，但是无法使用sql操作
  * 1.继承Aggregator,设置泛型
  * 2.实现方法
  */
class MyAgeAvgClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  //聚合数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  //缓冲区合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
