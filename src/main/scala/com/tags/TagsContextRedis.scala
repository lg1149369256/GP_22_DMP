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

    // 关键字广播出去
    val stopKeyWords: collection.Map[String, Int] = sc.textFile(stopPath).map((_, 0)).collectAsMap
    val bcstopKeyWords: Broadcast[collection.Map[String, Int]] = sc.broadcast(stopKeyWords)

    // 读取数据
    val df: DataFrame = spark.read.parquet(inputPath)
    val tups: RDD[(String, List[(String, Int)])] = df.filter(TagsUtils.hasneedOneUserId).rdd
      .mapPartitions( x =>{x.map(row => {
      val jedis = JedisConPool.getConnection()

      val userId = TagsUtils.getOneUserId(row)
      val adList = TagsAd.makeTags(row)
      val appList = TagsAppRedis.makeTags(row,jedis)
      val channelList = TagsChannel.makeTags(row)
      val keyWordsList = TagsKeyWords.makeTags(row,bcstopKeyWords)
      val areaList = TagsArea.makeTags(row)
      val buinessList = TagsBusiness.makeTags(row)

       jedis.close()
      (userId, adList ++ appList ++ channelList ++ keyWordsList ++ areaList++buinessList)
      })
   })

    val aggrUserTags = tups.reduceByKey((list1,list2) => {
      // List(("LN插屏",1),("LN全屏",1),("ZP河南",1),("ZC商丘",1)...)
      (list1 ::: list2)
        .groupBy(_._1).mapValues(x => x.foldLeft(0)((x, y) => (x + y._2))).toList
    })
     aggrUserTags.saveAsTextFile("d://output2019-0824")

    // aggrUserTags.saveAsTextFile("d://output2019-0823")
    //println(aggrUserTags.collect.toBuffer)
    // println(res.collect.toBuffer)


    spark.stop()
    sc.stop()
  }
}
