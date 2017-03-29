package cn.xukai.spark.es.write

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by xukai on 2017/3/28.
  */
object Write2Es {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .set("es.nodes","localhost")
      .set("es.port","9200")
      .setMaster("local")
      .setAppName("SparkOnES")

    val sc = new SparkContext(conf)
    //departure 距离
//    case class Trip(departure:String,arrival:String)

//    val upcomingTrip = Trip("OTP","SFO")
//    val lastWeekTrip = Trip("MUC","OTP")
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")
    val rdd = sc.makeRDD(Seq(numbers,airports))
    EsSpark.saveToEs(rdd,"spark/docs")
  }
}
