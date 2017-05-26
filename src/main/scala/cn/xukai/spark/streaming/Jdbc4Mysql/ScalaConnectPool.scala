package cn.xukai.spark.streaming.Jdbc4Mysql

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Date

import org.apache.log4j.Logger
import org.apache.commons.dbcp.BasicDataSource

/**
  * Created by kaixu on 2017/5/26.
  */
object ScalaConnectPool {
  val log = Logger.getLogger(ScalaConnectPool.this.getClass)
  var ds:BasicDataSource = null

  def getDataSource={
    if(ds == null){
      ds = new BasicDataSource()
      ds.setUsername("hive")
      ds.setPassword("hive")
      ds.setUrl("jdbc:mysql://hadoop-1:3306/test")
      ds.setDriverClassName("com.mysql.jdbc.Driver")
      ds.setInitialSize(20)
      ds.setMaxActive(100)
      ds.setMinIdle(50)
      ds.setMaxIdle(100)
      ds.setMaxWait(1000)
      ds.setMinEvictableIdleTimeMillis(5*60*1000)
      ds.setTimeBetweenEvictionRunsMillis(10*60*1000)
      ds.setTestOnBorrow(true)
    }
    ds
  }

  def getConnection : Connection= {
    var connect:Connection = null
    try {
      if(ds != null){
        connect = ds.getConnection
      }else{
        connect = getDataSource.getConnection
      }
    }
    connect
  }
  def shutDownDataSource: Unit=if (ds !=null){ds.close()}

  def closeConnection(rs:ResultSet,ps:PreparedStatement,connect:Connection): Unit ={
    if(rs != null){rs.close}
    if(ps != null){ps.close}
    if(connect != null){connect.close}
  }

  def now(): Date ={
    new Date()
  }

  def main(args: Array[String]): Unit = {
    val conn = ScalaConnectPool.getConnection
    conn.setAutoCommit(false)
    val stmt = conn.createStatement()

    stmt.addBatch("insert into searchKeyWord (insert_time,keyword,search_count) values (now(),'jjj',22)")

    stmt.executeBatch()
    conn.commit()

    stmt.close()
    conn.close()
    println("is end ...")

  }
}
