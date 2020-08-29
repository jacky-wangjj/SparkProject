package sparkCore.baseOpt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkRDDTest").setMaster("local")
    val sc = new SparkContext(conf)
    //生成RDD有两种方式：
    //1.sc.parallelize()
    //2.sc.textFile()
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9)).map(_ * 3)
    val filtered: Array[Int] = rdd.filter(_ > 10).collect()
    //对集合求和
    println(rdd.reduce(_ + _))
    //输出大于10的元素
    for (arg <- filtered) {
      print(arg + " ")
    }
    println()
    //通过并行化生产rdd
    val rdd1 = sc.parallelize(List(5, 6, 4, 7, 3, 8, 2, 9, 1, 10))
    //对rdd1里的每个元素乘以2然后排序
    val res1 = rdd1.map(_ * 2).sortBy(x => x, true)
    println(res1.collect().toBuffer)

    //过滤出大于等于十的元素
    val res2 = res1.filter(_ >= 10)
    //将元素以数组的方式打印出来
    println(res2.collect().toBuffer)

    val rdd2 = sc.parallelize(Array("a b c", "d e f", "h i j"))
    //将rdd2里面的每一个元素先切分再压平
    val res3 = rdd2.flatMap(_.split(" "))
    println(res3.collect().toBuffer)

    val rdd3 = sc.parallelize(List(List("a b c", "a b b"), List("e f g", "a f g"), List("h i j", "a a b")))
    //将rdd3里面的每个元素鲜切分再压平
    val res4 = rdd3.flatMap(_.flatMap(_.split(" ")))
    println(res4.collect().toBuffer)

    val rdd4 = sc.parallelize(List(5, 6, 4, 3))
    val rdd5 = sc.parallelize(List(1, 2, 3, 4))
    //求并集
    val res5 = rdd4.union(rdd5)
    println(res5.collect().toBuffer)
    //求交集
    val res6 = rdd4.intersection(rdd5)
    println(res6.collect().toBuffer)
    //去重
    val res7 = rdd4.union(rdd5).distinct()
    println(res7.collect().toBuffer)

    val rdd6 = sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2)))
    val rdd7 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
    //求join
    val res8 = rdd6.join(rdd7)
    println(res8.collect().toBuffer)

    //求左连接和右连接
    val res9 = rdd6.leftOuterJoin(rdd7)
    println(res9.collect().toBuffer)
    val res10 = rdd6.rightOuterJoin(rdd7)
    println(res10.collect().toBuffer)
    //求并集
    val res11 = rdd6.union(rdd7)
    println(res11.collect().toBuffer)

    //按key进行分组
    val res12 = res11.groupByKey()
    println(res12.collect().toBuffer)

    //分别用groupByKey和reduceByKey实现单词计数，注意groupByKey和reduceByKey的区别
    //groupByKey
    val res13 = res11.groupByKey().mapValues(_.sum)
    println(res13.collect().toBuffer)
    //reduceByKey 先进行聚合在进行shuffle
    val res14 = res11.reduceByKey(_ + _)
    println(res14.collect().toBuffer)

    val rdd8 = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
    val rdd9 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
    //cogroup 注意cogroup与groupByKey的区别
    val res15 = rdd8.cogroup(rdd9)
    println(res15.collect().toBuffer)

    val rdd10 = sc.parallelize(List(1, 2, 3, 4, 5))
    //reduce聚合
    rdd10.reduce(_ + _)


    val rdd11 = sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2), ("shuke", 1)))
    val rdd12 = sc.parallelize(List(("jerry", 2), ("tom", 3), ("shuke", 2), ("kitty", 5)))
    val rdd13 = rdd11.union(rdd12)
    //按key进行聚合 reduceByKey

    //按value的降序排序
    val res16 = rdd13.reduceByKey(_ + _).map(t => (t._2, t._1)).sortByKey(false).map(t => (t._2, t._1))
    println(res16.collect().toBuffer)
    //笛卡尔积
    val res17 = rdd11.cartesian(rdd12)
    println(res17.collect().toBuffer)
    //其他count、top、take、first、takeOrdered

  }
}
