package cn.xukai.spark.streaming.Jdbc4Mysql

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by kaixu on 2017/5/26.
  */
object DataToMySQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("use the foreachRDD write data to mysql”).setMaster(“local[2]")
    val ssc = new StreamingContext(conf,Seconds(10))
    val streamData = ssc.socketTextStream("hadoop-3",9999)
    val wordCount = streamData.map(line =>(line.split(",")(0),1)).reduceByKeyAndWindow(_+_,Seconds(60))

    val hottestWord = wordCount.transform(itemRDD => {
      // 对value降序，并且取前3
      val top3 = itemRDD.map(pair => (pair._2, pair._1))
        .sortByKey(false).map(pair => (pair._2, pair._1)).take(3)
      ssc.sparkContext.makeRDD(top3)
    })

    //持久化操作
    hottestWord.foreachRDD( rdd =>{
      rdd.foreachPartition(partitionOfRecords =>{
        val connect = ScalaConnectPool.getConnection
        connect.setAutoCommit(false)
        val stmt = connect.createStatement()
        partitionOfRecords.foreach(record =>{
          stmt.addBatch("insert into searchKeyWord (insert_time,keyword,search_count) values (now(),'"+record._1+"','"+record._2+"')")
        })
        stmt.executeBatch()
        connect.commit()
      }
      )
    }
    )
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
