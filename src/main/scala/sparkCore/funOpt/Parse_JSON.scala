package sparkCore.funOpt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON

object Parse_JSON {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    //textFile读取文件时，要求每一行是一个json串{key:value,key1:value1}
    val jsonRDD: RDD[String] = sc.parallelize(Array("{\"name\":\"zhangsan\",\"age\":20}", "{\"name\":\"lisi\",\"age\":25}"))

    val json: RDD[Option[Any]] = jsonRDD.map(JSON.parseFull)

    println(json.collect().toBuffer)
    //结束任务
    sc.stop()
  }
}
