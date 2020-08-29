package sparkCore.funOpt

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Conn_HBase {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)
    //需拷贝hbase-site.xml至资源目录
    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")
    /**
      * 读取hbase数据
      */
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    hbaseRDD.foreach {
      case (rowkey, result) => {
        val cells: Array[Cell] = result.rawCells()
        for (cell <- cells) {
          println(Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }

    /**
      * 写入hbase数据
      */
    val dataRDD: RDD[(String, String)] = sc.makeRDD(List(("1002", "zhangsan"), ("1003", "lisi")))
    val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
      case (rowkey, name) => {
        val put: Put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))

        (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
      }
    }
    val jobConf: JobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "student")

    putRDD.saveAsHadoopDataset(jobConf)

    //结束任务
    sc.stop()
  }
}
