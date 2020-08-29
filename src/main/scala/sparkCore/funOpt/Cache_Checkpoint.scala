package sparkCore.funOpt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Cache_Checkpoint {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    //设定检查点的保存目录
    sc.setCheckpointDir("checkpoint")
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "hbase"))

    val mapRDD: RDD[(String, Int)] = rdd.map((_, 1))

    //默认为MEMORY_ONLY
    //DISK_ONLY_2 只存磁盘 2个副本
    //OFF_HEAP 堆外内存 优化GC出现，用户可以严格管理内存
    //    mapRDD.persist(StorageLevel.MEMORY_ONLY)
    //    mapRDD.cache()

    println("map Lineage:\n" + mapRDD.toDebugString)

    val res: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    //保存检查点
    res.checkpoint()

    println("Lineage:\n" + res.toDebugString)
    println(res.collect().toBuffer)
    println("Lineage:\n" + res.toDebugString)

    //结束任务
    sc.stop()
  }
}
