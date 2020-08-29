package sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SQL_DataFrame {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL_DataFrame")
    //设置spark.sql.warehouse.dir的路径
    conf.set("spark.sql.warehouse.dir", "file:/D:/ML/Code/SparkProject/src/main/scala/sparksql/schema")
    //创建SparkSession
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val dataFrame: DataFrame = sparkSession.read.json("D:\\ML\\Code\\SparkProject\\src\\main\\scala\\sparkSql\\input\\user.json")
    //将DataFrame转换为视图表
    dataFrame.createOrReplaceTempView("user")
    //sql查询访问
    sparkSession.sql("select * from user").show()

    //释放资源
    sparkSession.stop()
  }
}
