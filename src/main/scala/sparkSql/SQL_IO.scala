package sparkSql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SQL_IO {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL_DataFrame")
    //设置spark.sql.warehouse.dir的路径
    conf.set("spark.sql.warehouse.dir", "file:/D:/ML/Code/SparkProject/src/main/scala/sparksql/schema")
    //创建SparkSession
    val sparkSession: SparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport() //spark操作hive
      .getOrCreate()
    //使用rdd.toDF需先引入隐式转换规则
    import sparkSession.implicits._
    //读写json文件
    val df: DataFrame = sparkSession.read.json("D:\\ML\\Code\\SparkProject\\src\\main\\scala\\sparkSql\\input\\user.json")
    df.show()
    sparkSession.read.format("json").load("D:\\ML\\Code\\SparkProject\\src\\main\\scala\\sparkSql\\input\\user.json").show()
    df.write.format("json").save("D:\\ML\\Code\\SparkProject\\src\\main\\scala\\sparkSql\\output")
    df.write.format("json").mode("append").save("D:\\ML\\Code\\SparkProject\\src\\main\\scala\\sparkSql\\output")

    //读写jdbc
    val jdbcURL = "jdbc:mysql://localhost:3306/testdb?useSSL=false&autoReconnect=true"
    val table = "person"
    val jdbcDF: DataFrame = sparkSession.read.format("jdbc")
      .option("url", jdbcURL)
      .option("dbtable", table)
      .option("user", "root")
      .option("password", "123456").load()
    jdbcDF.show()
    //使用Properties读取
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("url", jdbcURL)
    val jdbcDF1: DataFrame = sparkSession.read.jdbc(jdbcURL, table, prop)
    jdbcDF1.show()

    //DataFrame写入mysql
    jdbcDF.write.jdbc(jdbcURL, table, prop)

    //hive读取，默认支持内置hive
    //配置集群hive，拷贝hive-site.xml至spark的classpath。即resources下
    sparkSession.sql("show tables").show()
  }
}
