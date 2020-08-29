package sparkStreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver

/**
  * nc -l -p 8888
  */
object Streaming_MyReceiver {
  def main(args: Array[String]): Unit = {
    //获取StreamingContext
    val conf = new SparkConf().setAppName("StreamingWC").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置检查点
    ssc.checkpoint("file:/D:/data/checkpoint")
    //从自定义采集器中采集数据
    val receiverDStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("localhost", 8888))
    //扁平化，转换tuple
    val mapDStream: DStream[(String, Int)] = receiverDStream.flatMap(_.split(" ")).map((_, 1))
    //聚合数据
    val res: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)
    //打印结果
    res.print()
    //启动采集器
    ssc.start()
    //Driver等待采集器的执行
    ssc.awaitTermination()
  }
}

/**
  * 自定义Receiver
  */
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  private var socket: Socket = null

  def receive(): Any = {
    socket = new Socket(host, port)
    val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
    var line: String = null
    while ((line = reader.readLine()) != null) {
      if ("END".equals(line)) {
        return
      } else {
        //将采集的数据存储到采集器的内部进行转换
        this.store(line)
      }

    }
  }

  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    }).start()
  }

  override def onStop(): Unit = {
    if (socket != null) {
      socket.close()
      socket = null
    }
  }
}
