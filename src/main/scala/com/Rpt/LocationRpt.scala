package com.Rpt

import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext}

/**
  * 地域分布指标
  */
object LocationRpt {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    // 判断路径是否正确
    if(args.length != 1){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 获取数据
    val df = sQLContext.read.parquet(inputPath)
    val tups: RDD[((String, String), List[Double])] = df.rdd.map(row => {

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
      // key值是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")

      // 创建三个对应的方法处理九个指标
      val s = RptUtils.request(requestmode, processnode)
      // 展示数，点击数
      val a = RptUtils.click(requestmode, iseffective)
      // 参与竞价数，竞价成功数，广告消费，广告成本
      val d = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)

      ((pro, city), s ++ a ++ d)
    })
    // 将数据进行处理，统计各个指标

    //println(tups.collect.toBuffer)
    val sumed: RDD[((String, String), List[Double])] = tups.reduceByKey((list1, list2) => list1.zip(list2).map(x => x._2 + x._1))

    val res = sumed.map(x =>{
      val pro = x._1._1
      val city = x._1._2
      val list = x._2.mkString(",")
      (pro,city,list)
    })
   println(res.collect.toBuffer)

    // 如果想要存入mysql的话，需要使用foreachPartition
    // 需要自己写一个连接池
    sc.stop()
  }
}
