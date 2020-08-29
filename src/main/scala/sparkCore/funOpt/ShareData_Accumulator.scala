package sparkCore.funOpt

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

object ShareData_Accumulator {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    //创建累加器对象
    val accumulator: LongAccumulator = sc.longAccumulator

    dataRDD.foreach {
      case i => {
        //执行累加操作
        accumulator.add(i)
      }
    }
    //获取累加器的值
    println("sum:" + accumulator.value)

    /**
      * 自定义累加器
      */
    val stringRDD: RDD[String] = sc.makeRDD(List("hadoop", "hive", "spark"))
    //创建累加器
    val myaccumulator: MyAccumulator = new MyAccumulator
    //注册累加器
    sc.register(myaccumulator)
    stringRDD.foreach {
      case word => {
        //执行累加操作
        myaccumulator.add(word)
      }
    }
    //获取累加器的值
    println("sum:" + myaccumulator.value)

    //结束任务
    sc.stop()
  }
}

/**
  * 声明累加器
  * 1.继承AccumulatorV2
  * 2.实现抽象方法
  * 3.创建累加器
  */
class MyAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  private val list: util.ArrayList[String] = new util.ArrayList[String]()

  //当前的累加器是否为初始化状态
  override def isZero: Boolean = list.isEmpty

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new MyAccumulator()
  }

  //重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  //向累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.add(v)
    }
  }

  //合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  //获取累加器的结果
  override def value: util.ArrayList[String] = list
}
