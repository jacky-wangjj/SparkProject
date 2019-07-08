package example1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * data.txt中数据为uid,输入的完整句子,简写提示的句子
  * 计算每个用户的输入效率：简写提示的句子的长度/输入的完整句子的长度
  * 需要先将输入的完整句子和简写提示的句子按照uid进行聚合累加，之后计算输入效率
  */
object SparkExample1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkExample1").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("D:\\ML\\Code\\SparkProject\\src\\main\\scala\\example1\\input\\data.txt")
    //line -> Array(Array(1,happy birthday,hapb),Array(2,hello world,helw),Array(3,hello spark,helsp))
    val fields = lines.map(_.split(","))
    //取数组中的值时使用x(0)，下标从0开始
    //x=Array(1,happy birthday,hapb) -> Tuple(1,(13,4))
    val lens = fields.map(x => (x(0), (x(1).length, x(2).length)))
    //取Tuple中的数据时使用x._1，下标从1开始
    //x=(13,4),y=(10,4) -> (23,8)
    val len = lens.reduceByKey((x, y) => ((x._1 + y._1), (x._2 + y._2)))
    //x=(1,(23,8)) -> (1, 23/8)
    val res = len.map(x => (x._1, (x._2._2.toDouble / x._2._1.toDouble))).sortByKey(true)

    println(res.collect().toBuffer)
    sc.stop()
  }
}
