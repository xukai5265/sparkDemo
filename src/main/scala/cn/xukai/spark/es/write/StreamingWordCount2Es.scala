package cn.xukai.spark.es.write

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by xukai on 2017/3/29.
  */
object StreamingWordCount2Es extends App{
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)

  val conf = new SparkConf()
    .setAppName("StreamingWordCount2Es")
    .set("es.nodes","localhost")
    .set("es.port","9200")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc,Seconds(10))
  ssc.checkpoint("hdfs://192.168.137.140:9000/tmp")
  val lines = ssc.socketTextStream("localhost",9999)
//  lines.foreachRDD(rdd =>{
//    val wordCount = rdd.flatMap(_.split(" ")).map(line => (line,1)).reduceByKey(_+_)
//      EsSpark.saveToEs(wordCount,"streaming/wordcount")
//  })
  val wordcount = lines.flatMap(_.split(" ")).map(word=>(word,1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(30), Seconds(10), 2)
  wordcount.foreachRDD(rdd =>{
    val numbers = Map("one" -> rdd)
    val rdd1 = sc.makeRDD(Seq(numbers))
    EsSpark.saveToEs(rdd1,"streaming/wordcount")
  })
  wordcount.print()
  ssc.start()
  ssc.awaitTermination()
}
