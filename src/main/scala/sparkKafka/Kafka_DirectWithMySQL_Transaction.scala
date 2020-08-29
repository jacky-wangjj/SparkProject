package sparkKafka

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.DB

import scala.collection.mutable

/**
  * 原子处理，将偏移量存储在事务里面
  * 为了使获得的结果保持输出一致性语义，你用来保存结果和偏移量到外部数据存储的操作必须是幂等或者是原子事务。
  * 建表：
  * create table mytopic(topic varchar(200), partid int, offset bigint);
  * create table mydata(name varchar(200), id int);
  * 插入offset初始值
  * insert into mytopic(topic, partid, offset) values('mytopic',0,0);
  * insert into mytopic(topic, partid, offset) values('mytopic',1,0);
  * insert into mytopic(topic, partid, offset) values('mytopic',2,0);
  *
  * https://blog.csdn.net/a805814077/article/details/106531020
  */
object Kafka_DirectWithMySQL_Transaction {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf()
      .setAppName("Kafka_DirectWithMySQL_Transaction")
      .setMaster("local[*]")
    //是否启用SparkStreaming内部的backpressure机制
    //当batch processing time>batch interval这种情况持续过长的时间，会造成数据在内存中堆积，导致receiver所在Executor内存溢出等问题
    //      .set("spark.streaming.backpressure.enabled", "false")
    //设置spark程序每秒中从每个partition分区读取的最大的数据条数
    //      .set("spark.streaming.kafka.maxRatePerPartition", "100")
    val batchInterval = Seconds(5)

    val topic: String = "test"
    val customGroup: String = "testGroup"
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> "127.0.0.1:9092",
      "group.id" -> customGroup,
      "auto.offset.reset" -> "smallest",
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )
    //创建一个set,里面放入你要读取的Topic
    val topics: Set[String] = Set[String](topic)
    //上下文对象
    val ssc: StreamingContext = new StreamingContext(conf, batchInterval)
    //jdbc相关配置信息
    val jdbcURL = "jdbc:mysql://localhost:3306/testdb?useSSL=false&autoReconnect=true"
    val jdbcDriver = "com.mysql.jdbc.Driver"
    val jdbcUser = "root"
    val jdbcPassword = "123456"
    val offsets = mutable.Map[TopicAndPartition, Long]()
    //注册Driver
    Class.forName(jdbcDriver)
    //获取连接
    val connection: Connection = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPassword)
    val getoffset: PreparedStatement = connection.prepareStatement("select topic, partid, offset from mytopic")
    val setdata: PreparedStatement = connection.prepareStatement("insert into mydata(name,id) values (?,?)")
    val setoffset: PreparedStatement = connection.prepareStatement("update mytopic set offset = ? where topic = ? and partid = ?")
    val results: ResultSet = getoffset.executeQuery()
    while (results.next()) {
      offsets.put(TopicAndPartition(results.getString(1), results.getInt(2)), results.getLong(3))
    }
    val messageHandler: MessageAndMetadata[String, String] => (String, String) = (msgH: MessageAndMetadata[String, String]) => (msgH.key(), msgH.message())
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
      ssc,
      kafkaParams,
      offsets.toMap,
      messageHandler
    )
    messages.foreachRDD((rdd, time) => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          //获取偏移量
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          //获取当前partition所对应的偏移量
          val pOffsetRang: OffsetRange = offsetRanges(TaskContext.get().partitionId())
          // scala中使用localTx 开启事务操作
          DB.localTx { implicit session =>
            //数据
            partition.foreach(msg => {
              // 或者使用scalike的batch 插入
              val name = msg._2.split(",")(0)
              val id = msg._2.split(",")(1)
              setdata.setString(1, name)
              setdata.setInt(2, id.toInt)
              val res: Int = setdata.executeUpdate()
            })
            //            val i = 1 / 0//测试，若发生错误，则更新失败，进行事务的回滚操作
            //偏移量
            setoffset.setLong(1, pOffsetRang.untilOffset)
            setoffset.setString(2, pOffsetRang.topic)
            setoffset.setInt(3, pOffsetRang.partition)
            val res: Int = setoffset.executeUpdate()
          }
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()
    //    ssc.stop()
  }
}
