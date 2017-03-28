package cn.xukai.spark.es.write

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by xukai on 2017/3/28.
  */
object Write2Es {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .set("es.nodes","192.168.137.40")
      .set("es.port","9200")
      .setMaster("local")
      .setAppName("SparkOnES")

    val sc = new SparkContext(conf)
    //departure 距离
    case class Trip(departure:String,arrival:String)

    val upcomingTrip = Trip("OTP","SFO")
    val lastWeekTrip = Trip("MUC","OTP")
    val rdd = sc.makeRDD(Seq(upcomingTrip,lastWeekTrip))
    EsSpark.saveToEs(rdd,"spark/docs")
  }
}
