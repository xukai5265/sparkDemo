package cn.xukai.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xukai on 17-3-16.
  */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    val logFile = "file:///application/datasource/result/record.list"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("spark://localhost:7077")
      .setJars(Seq("./target/sparkDemo-1.0-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val rdd = logData.flatMap(line => line.split(",")).map(line => (line,1)).reduceByKey(_+_)
    rdd.saveAsTextFile("file:///application/datasource/result/xukai.txt")
  }
}
