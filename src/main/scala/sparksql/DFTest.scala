package sparksql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

case class Person(id: Int, name: String, age: Int, faceValue: Int)

//通过反射创建DataFram
object DFTest {
  def main(args: Array[String]): Unit = {
    //获取SparkSession
    val conf = new SparkConf().setAppName("DFTest2").setMaster("local")
    conf.set("spark.sql.warehouse.dir", "file:/D:/ML/Code/SparkProject/src/main/scala/sparksql/schema")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    //获取数据
    val lineRDD = sparkSession.sparkContext.textFile("D:\\ML\\Code\\SparkProject\\src\\main\\scala\\sparksql\\input\\data.txt")
    val personRDD = lineRDD.map(_.split(",")).map(x => Person(x(0).toInt, x(1), x(2).toInt, x(3).toInt))
    //创建DataFrame，导入隐式转换
    import sparkSession.implicits._
    val personDF = personRDD.toDF
    //打印DataFrame
    personDF.show()

    //DSL查询
    //查询的三种写法
    personDF.select(personDF.col("name"), personDF.col("age")).show()
    personDF.select("name", "age").show()
    personDF.select(personDF("name"), personDF("age")).show()
    personDF.select(personDF("name"), personDF("age") + 1).show()
    //查询年龄大于25的所有数据
    personDF.filter(personDF("age") >= 25).show()
    //按年龄进行分组，统计相同年龄的人数
    personDF.groupBy(personDF("age")).count().show()

    //SQL查询
    //创建临时表
    personDF.createTempView("t_person")
    //查询
    val resultDF = sparkSession.sql("select * from t_person order by age desc limit 2")
    //输出
    resultDF.show()
    //    resultDF.write.json(args(1))
    //关闭资源
    sparkSession.stop()
  }
}
