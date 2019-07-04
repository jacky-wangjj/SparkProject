import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * BaseOpt Created by wangjj on 2019/5/13 15:30
  */
class MyClass {
  val field = "Hello"

  def doStuff(rdd: RDD[String]): RDD[String] = {
    val field_ = this.field
    rdd.map(x => field_ + x)
  }
}

object Myfunctions {
  def GetLength(s: String): Integer = {
    return s.length
  }

  def Sum(a: Integer, b: Integer): Integer = {
    return a + b;
  }
}

object BaseOpt {
  def parallelize(sc: SparkContext): Unit = {
    val data = Array(1, 2, 3, 4, 5, 6)
    val distData = sc.parallelize(data)
  }

  def hellFile(sc: SparkContext): Unit = {
    val myClass: MyClass = new MyClass
    val distFile = sc.textFile("pom.xml")
    val helloFile = myClass.doStuff(distFile)
    helloFile.collect().foreach(println(_)) //collect()将所有RDD放到driver程序节点上，可能导致driver程序耗尽内存
    helloFile.take(100).foreach(println) //take(100)只打印100个元素
  }

  //需保证所有工作节点在相同的路径下可以访问到文件
  def textFile(sc: SparkContext): Unit = {
    val distFile = sc.textFile("pom.xml")
    //    val lineLengths = distFile.map(s => s.length)
    val lineLengths = distFile.map(Myfunctions.GetLength)
    //    val totalLength = lineLengths.reduce((a, b) => a + b)
    val totalLength = lineLengths.reduce(Myfunctions.Sum)
    //    lineLengths.persist()
    println(s"totalLength: $totalLength")
  }

  def reduceByKey(sc: SparkContext): Unit = {
    val lines = sc.textFile("pom.xml")
    val pairs = lines.map(s => (s, 1))
    val counts = pairs.reduceByKey((a, b) => a + b)
    counts.sortByKey().collect().foreach(println)
  }

  def broadcast(sc: SparkContext): Unit = {
    val b = sc.broadcast(Array(1, 2, 3))
    println(b.value)
  }

  def accumulator(sc: SparkContext): Unit = {
    val accum = sc.longAccumulator("my accumulator")
    sc.parallelize(Array(1, 2, 3, 4, 5)).foreach(x => accum.add(x))
    println(accum.value)
  }

  def main(args: Array[String]) {
    val logFile = "pom.xml"
    val conf = new SparkConf().setAppName("mySpark").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile).cache()
    val numOs = logData.filter(line => line.contains("o")).count()
    println(s"Lines with o: $numOs")
    hellFile(sc)
    textFile(sc)
    reduceByKey(sc)
    broadcast(sc)
    accumulator(sc)
    sc.stop()
  }
}