package sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
  * 用户自定义聚合函数
  */
object SQL_UDAF {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL_DataFrame")
    //设置spark.sql.warehouse.dir的路径
    conf.set("spark.sql.warehouse.dir", "file:/D:/ML/Code/SparkProject/src/main/scala/sparksql/schema")
    //创建SparkSession
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //使用rdd.toDF需先引入隐式转换规则
    import sparkSession.implicits._
    //创建聚合函数对象
    val udaf: MyAgeAvgFunction = new MyAgeAvgFunction
    sparkSession.udf.register("avgAge", udaf)
    //使用聚合函数
    //创建RDD
    val rdd: RDD[(Int, String, Int)] = sparkSession.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    //rdd转换为DataFrame
    val df: DataFrame = rdd.toDF("id", "name", "age")
    df.createOrReplaceTempView("user")
    sparkSession.sql("select avgAge(age) from user").show()
    //释放资源
    sparkSession.stop()
  }
}

/**
  * 声明用户自定义聚合函数 弱类型，需依据参数位置获取参数
  * 1.继承UserDefinedAggregateFunction
  * 2.实现方法
  */
class MyAgeAvgFunction extends UserDefinedAggregateFunction {
  //函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  //函数返回的数据类型
  override def dataType: DataType = DoubleType

  //函数是否稳定
  override def deterministic: Boolean = true

  //计算之前的缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //根据查询结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
