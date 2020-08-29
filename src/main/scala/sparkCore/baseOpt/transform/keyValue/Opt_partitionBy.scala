package sparkCore.baseOpt.transform.keyValue

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 只有K V类型的RDD才有分区器，非K V类型的RDD分区器的值是None
  * 每个RDD分区id范围：0 - numPartitions-1
  */
object Opt_partitionBy {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (1, "ccc"), (4, "ddd")), 4)
    println("分区前：" + rdd.partitions.size)
    //shuffle操作
    //参数 分区器 HashPartitioner要求分区为2*n
    val rdd2: RDD[(Int, String)] = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))
    rdd2.glom().foreach(array => {
      println("HashPartitioner:" + array.mkString(","))
    })
    //自定义分区器
    val rdd3: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))
    rdd3.glom().foreach(array => {
      println("MyPartitioner:" + array.mkString(","))
    })
    //结束任务
    sc.stop()
  }
}

/**
  * 自定义分区器
  */
class MyPartitioner(partitions: Int) extends Partitioner {
  //返回创建出来的分区数
  override def numPartitions: Int = partitions

  //返回给定键的分区编号0-numPartitions-1
  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => 1
  }
}
