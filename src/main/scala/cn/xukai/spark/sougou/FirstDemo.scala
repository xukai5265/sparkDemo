package cn.xukai.spark.sougou

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kaixu on 2017/5/24.
  */
object FirstDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark test sg ")
    val sc = new SparkContext(conf)
    val sgData = sc.textFile("hdfs://tianxi/test/data.txt")
    val sgData1 = sgData.map(_.replace(" ","\t"))
    val etlData = sgData1.map(_.split("\t")).filter(_.length == 6)
    val ss = etlData.filter(_(3).toInt==1).filter(_(4).toInt==1)

    ss.saveAsTextFile("hdfs://tianxi/test/k")
  }
}
