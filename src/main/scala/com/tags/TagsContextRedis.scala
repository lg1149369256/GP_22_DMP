package com.tags

import com.jedisConPool.JedisConPool
import com.utils.{TagsArea, TagsUtils}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TagsContextRedis {
  def main (args: Array[String]): Unit = {
    if(args.length != 3){
      println("目录不正确，退出程序！！！")
      sys.exit()
    }
    val Array(inputPath,inputPath2,stopPath) = args
    // 创建上线文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

//    val tup = sc.textFile(inputPath2).map(_.split("\t")).filter(x => x.length > 5)
//      .map(x => {
//        val appid1 = x(4)
//        val appname = x(1)
//        (appid1, appname)
//      }).foreachPartition(it => {
//        val jedis = JedisConPool.getConnection()
//        it.foreach(t => {
//          jedis.set(t._1, t._2)
//        })
//        jedis.close()
//    })

    val stopKeyWords: collection.Map[String, Int] = sc.textFile(stopPath).map((_, 0)).collectAsMap
    val bcstopKeyWords: Broadcast[collection.Map[String, Int]] = sc.broadcast(stopKeyWords)
    // 关键字广播出去


    // 读取数据
    val df: DataFrame = spark.read.parquet(inputPath)
    val tups: RDD[(String, List[(String, Int)])] = df.filter(TagsUtils.hasneedOneUserId).rdd
      .mapPartitions( x => x
      // 接下来所有的标签都在内部实现
      .map(row => {
      // 取出用户id
      val userId = TagsUtils.getOneUserId(row)

      // 接下来通过row数据，打上广告位类型的所有标签
      val adList = TagsAd.makeTags(row)

      // 打上App所有标签
      val appList = TagsAppRedis.makeTags(row, JedisConPool.getConnection())

      // 打渠道标签
      val channelList = TagsChannel.makeTags(row)

      // 打关键字标签
      val keyWordsList = TagsKeyWords.makeTags(row,bcstopKeyWords)

      // 地域标签
      val areaList = TagsArea.makeTags(row)

      (userId, adList ++ appList ++ channelList ++ keyWordsList ++ areaList)
    }))

    val aggrUserTags = tups.reduceByKey((list1,list2) => {
      (list1 ::: list2).groupBy(_._1).mapValues(x => x.foldLeft(0)((x, y) => (x + y._2))).toList
    })
     aggrUserTags.saveAsTextFile("d://output823")

    // aggrUserTags.saveAsTextFile("d://output2019-0823")
    //println(aggrUserTags.collect.toBuffer)
    // println(res.collect.toBuffer)


    spark.stop()
    sc.stop()
  }
}
