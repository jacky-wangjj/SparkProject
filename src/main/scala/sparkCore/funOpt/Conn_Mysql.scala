package sparkCore.funOpt

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Conn_Mysql {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transform opt")
    //创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/spark"
    val username = "root"
    val password = "123456"

    /**
      * 查询数据
      */
    //创建JdbcRDD,访问数据库
    val sql = "select name,age from user where id >= ? and id <= ?"
    val jdbcRDD: JdbcRDD[Unit] = new JdbcRDD(
      sc,
      () => {
        //获取数据库连接对象
        Class.forName(driver)
        java.sql.DriverManager.getConnection(url, username, password)
      },
      sql,
      1, //查询上线
      2, //查询下线
      2, //分区数
      (res) => {
        println(res.getString(1) + " " + res.getInt(2))
      }
    )
    jdbcRDD.collect()

    /**
      * 保存数据
      */
    val dataRDD: RDD[(Int, String, Int)] = sc.makeRDD(Array((4, "zhangsan", 30), (5, "lisi", 35)))
    //模式匹配
    dataRDD.foreachPartition { datas => {
      Class.forName(driver)
      val connection: Connection = java.sql.DriverManager.getConnection(url, username, password)
      val sql = "insert into user(id,name,age) values (?,?,?)"
      datas.foreach {
        case (id, name, age) => {
          val statement: PreparedStatement = connection.prepareStatement(sql)
          statement.setInt(1, id)
          statement.setString(2, name)
          statement.setInt(3, age)
          statement.executeUpdate()
          statement.close()
        }
      }
      connection.close()
    }
    }

    //结束任务
    sc.stop()
  }
}
