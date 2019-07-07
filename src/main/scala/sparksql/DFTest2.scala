package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//此方式实际工作中较多使用
object DFTest2 {
  def main(args: Array[String]): Unit = {
    //获取SparkSession
    val conf = new SparkConf().setAppName("DFTest2").setMaster("local")
    conf.set("spark.sql.warehouse.dir", "file:/D:/ML/Code/SparkProject/src/main/scala/sparksql/schema")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    //定义schema，带有StructType
    /*
        val schemaString = "id,name,age,faceValue"
        val fields = schemaString.split(",").map(field => StructField(field, StringType, nullable = true))
        val schema = StructType(fields)
    */
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("faceValue", IntegerType, true)
      )
    )
    //获取数据
    val lineRDD = sparkSession.sparkContext.textFile("D:\\ML\\Code\\SparkProject\\src\\main\\scala\\sparksql\\input\\data.txt")
    val personRDD = lineRDD.map(_.split(",")).map(x => Row(x(0).toInt, x(1), x(2).toInt, x(3).toInt))
    //创建DataFrame
    val personDF = sparkSession.createDataFrame(personRDD, schema)
    //打印DataFrame
    personDF.show()

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
