package sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SQL_Transform {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL_DataFrame")
    //设置spark.sql.warehouse.dir的路径
    conf.set("spark.sql.warehouse.dir", "file:/D:/ML/Code/SparkProject/src/main/scala/sparksql/schema")
    //创建SparkSession
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //使用rdd.toDF需先引入隐式转换规则
    import sparkSession.implicits._
    //创建RDD
    val rdd: RDD[(Int, String, Int)] = sparkSession.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    //rdd转换为DataFrame
    val df: DataFrame = rdd.toDF("id", "name", "age")
    //DataFrame转换为DataSet
    val ds: Dataset[User] = df.as[User]
    //DataSet转换为DataFrame
    val df1: DataFrame = ds.toDF()
    //DataFrame转换为RDD
    val rdd1: RDD[Row] = df1.rdd
    rdd1.foreach(row => {
      println(row.getString(1))
    })
    println(rdd1.collect().toBuffer)

    /**
      * RDD转RDD[User],再转DataFrame DataSet[User]
      */
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    //RDD[User]转DataFrame
    val userDF: DataFrame = userRDD.toDF()
    userDF.createOrReplaceTempView("user_df")
    sparkSession.sql("select * from user_df").show()
    //RDD[User]转Dataset
    val userDS: Dataset[User] = userRDD.toDS()
    val rdd2: RDD[User] = userDS.rdd
    println(rdd2.collect().toBuffer)
    //将DataSet转换为视图表
    userDS.createOrReplaceTempView("user")
    sparkSession.sql("select * from user").show()
    //释放资源
    sparkSession.stop()
  }
}

case class User(id: Int, name: String, age: Int)
