package sparkCore.funOpt

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Var_Broadcast {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 2), ("b", 3), ("c", 1), ("d", 9)), 4)

    val list: List[(String, Int)] = List(("a", 4), ("b", 8), ("c", 3))
    //可以使用广播变量减少数据的传输
    //1.构建广播变量
    val broadcastVar: Broadcast[List[(String, Int)]] = sc.broadcast(list)
    val rdd3: RDD[(String, (Int, Any))] = rdd1.map {
      case (key, value) => {
        var v2: Any = null
        //使用广播变量
        for (t <- broadcastVar.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }
    println("rdd3：" + rdd3.collect().toBuffer)

    //结束任务
    sc.stop()
  }
}
