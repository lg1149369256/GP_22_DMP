package com.Rpt

import com.utils.RptUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.spark_project.jetty.util.StringUtil

object MeiTiRpt {
  def main (args: Array[String]): Unit = {
    // 判断目录是否正确
    if(args.length != 2){
      println("目录不正确，退出！！！")
      sys.exit()
    }
    // 创建输入输出路径
    val Array(inputPath1,inputPath2) = args

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 读取字典文件数据，并切分，过滤，最后生成map集合
    val map = sc.textFile(inputPath2).map(_.split("\t")).filter(x => x.length > 5)
      .map(x => {
        val appid1 = x(4)
        val appname = x(1)
        (appid1, appname)
      }).collect.toMap

    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(map)

    // 加载清洗后的数据
    val df: DataFrame = spark.read.parquet(inputPath1)
    val tups: RDD[(String, List[Double])] = df.rdd.map(row => {
      // 把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")

      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      val appid = row.getAs[String]("appid")

      // 从map集合中取appname
      var appname = row.getAs[String]("appname")
      // 去字典文件中匹配
      if (StringUtil.isBlank("appname")) {
        appname = broadcast.value.getOrElse(appid, "No")
      }

      // 创建三个对应的方法处理九个指标
      val s = RptUtils.request(requestmode, processnode)
      // 展示数，点击数
      val a = RptUtils.click(requestmode, iseffective)
      // 参与竞价数，竞价成功数，广告消费，广告成本
      val d = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)

      (appname, s ++ a ++ d)
    })
    val sumed: RDD[(String, List[Double])] = tups.reduceByKey((list1, list2) => list1.zip(list2).map(x => (x._2 + x._1)))

    //println(sumed.collect.toBuffer)
    sumed.coalesce(1).saveAsTextFile("d://qianfeng/gp22Dmp/output_meiti")

    sc.stop()
    spark.stop()

  }
}
