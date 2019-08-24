package com.jedisConPool

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppJedis {
  def main (args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val tup = sc.textFile("D://qianfeng/gp22Dmp/项目day01/Spark用户画像分析/app_dict.txt")
      .map(_.split("\t")).filter(x => x.length > 5)
      .map(x => {
        val appid1 = x(4)
        val appname = x(1)
        (appid1, appname)
      }).foreachPartition(it => {
      val jedis = JedisConPool.getConnection()
      it.foreach(t => {
        jedis.set(t._1, t._2)
      })
      jedis.close()
    })
    sc.stop()
    spark.stop()
  }
}
