package sparkKafka

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._

/**
  * 通过伴生对象来访问类的实例，实现单例模式
  */
class HBaseUtils {
  private val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("zookeeper.znode.parent", "/hbase")
  hbaseConf.set("zookeeper.session.timeout", "6000000")
  private var conn: Connection = ConnectionFactory.createConnection(hbaseConf)

  def closeConn(): Unit = {
    if (null != conn) {
      conn.close()
    }
    conn = null
  }

  def getTable(tableName: String): Table = {
    var table: Table = null;
    try {
      conn.getTable(TableName.valueOf(tableName))
    } catch {
      case e: Exception => println(e)
    }
    return table
  }

  def insertOneColumn(tableName: String, rowkey: String, cf: String, qualifier: String, value: String): Unit = {
    val table: Table = getTable(tableName)
    val put: Put = new Put(rowkey.getBytes())
    put.addColumn(cf.getBytes(), qualifier.getBytes(), value.getBytes())
    table.put(put)
    table.close()
  }

  def getOneRow(tableName: String, rowkey: String, cf: String): Result = {
    val table: Table = getTable(tableName)
    val get: Get = new Get(rowkey.getBytes())
    get.addFamily(cf.getBytes())
    val result: Result = table.get(get)
    table.close()
    result
  }
}

object HBaseUtils {
  private val hBaseUtils: HBaseUtils = new HBaseUtils()

  def getHBaseUtils: HBaseUtils = {
    hBaseUtils
  }
}
