package com.tags


import com.utils.{TagsArea, TagsUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  *  上下文标签
  */
object TagsContext {
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

    val map = sc.textFile(inputPath2).map(_.split("\t")).filter(x => x.length > 5)
      .map(x => {
        val appid1 = x(4)
        val appname = x(1)
        (appid1, appname)
      }).collect.toMap
    val broadcastMap = sc.broadcast(map)


    val stopKeyWords: collection.Map[String, Int] = sc.textFile(stopPath).map((_, 0)).collectAsMap
    val bcstopKeyWords: Broadcast[collection.Map[String, Int]] = sc.broadcast(stopKeyWords)
    // 关键字广播出去


    // 读取数据
    val df: DataFrame = spark.read.parquet(inputPath)
    val tups: RDD[(String, List[(String, Int)])] = df.filter(TagsUtils.hasneedOneUserId).rdd
      // 接下来所有的标签都在内部实现
      .map(row => {
      // 取出用户id
      val userId = TagsUtils.getOneUserId(row)

      // 接下来通过row数据，打上广告位类型的所有标签
      val adList = TagsAd.makeTags(row)

      // 打上App所有标签
      val appList = TagsApp.makeTags(row, broadcastMap)

      // 打渠道标签
      val channelList = TagsChannel.makeTags(row)

      // 打关键字标签
      val keyWordsList = TagsKeyWords.makeTags(row, bcstopKeyWords)

      // 地域标签
      val areaList = TagsArea.makeTags(row)

      (userId, adList ++ appList ++ channelList ++ keyWordsList ++ areaList)
    })
    val res = tups.map(list => {
      val userId = list._1
      val list1 = list._2
      val a = list1.groupBy(_._1).mapValues(x =>x.foldLeft(0)((x,y)=>(x + y._2))).toList
      (userId, a)
    })
    println(res.collect.toBuffer)


    spark.stop()
    sc.stop()

  }
}
