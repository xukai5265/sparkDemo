package cn.xukai.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by xukai on 17-3-23.
  * step1. run spark master and worker
  * step2. submit job
  *   spark-submit --class cn.xukai.spark.streaming.StreamingHelloWorld --master local sparkDemo-1.0-SNAPSHOT.jar
  * step3. send a message
  *   nc -lk 9999
  *   hello world
  */
object StreamingHelloWorld extends App{
  val sc = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(sc,Seconds(1))

  // Create a DStream that will connect to hostname:port, like localhost:9999
  val lines = ssc.socketTextStream("localhost", 9999)

  // Split each line into words
  val words = lines.flatMap(_.split(" "))

  // Count each word in each batch
  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)

  // Print the first ten elements of each RDD generated in this DStream to the console
  wordCounts.print()

  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate
}
