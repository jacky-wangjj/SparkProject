package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 有状态转换
  */
object Streaming_UpdateState {
  def main(args: Array[String]): Unit = {
    //获取StreamingContext
    val conf = new SparkConf().setAppName("StreamingWC").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置检查点
    ssc.checkpoint("file:/D:/data/checkpoint")
    //从指定端口中采集数据
    val receiverDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
    //扁平化，转换tuple
    val mapDStream: DStream[(String, Int)] = receiverDStream.flatMap(_.split(" ")).map((_, 1))
    val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buffer) => {
        Option(buffer.getOrElse(0) + seq.sum)
      }
    }
    stateDStream.print()
    //启动采集器
    ssc.start()
    //Driver等待采集器的执行
    ssc.awaitTermination()
  }
}
