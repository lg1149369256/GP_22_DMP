package com.tags


import com.typesafe.config.ConfigFactory
import com.utils.{TagsArea, TagsUtils}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD


/**
  *  上下文标签
  */
object TagsContext3 {
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

    // todo 调用Hbase API
    // 加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TableName")
    // 创建Hadoop任务
    val configuration = sc.hadoopConfiguration
    configuration.set("ha.zookeeper.quorum",load.getString("hbase.host"))
    // 创建HbaseCOnnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbaseAdmin = hbconn.getAdmin
    // 判断表是否可用
    if(!hbaseAdmin.tableExists(TableName.valueOf(hbaseTableName))){
      // 创建表操作
      val tableDesctiptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDesctiptor.addFamily(descriptor)
      hbaseAdmin.createTable(tableDesctiptor)
      hbaseAdmin.close()
      hbconn.close()
    }
    // 创建JobConf
    val jobconf = new JobConf(configuration)
    // 指定输出类型
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    // 指定输出表
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)


    val map = sc.textFile(inputPath2).map(_.split("\t")).filter(x => x.length > 5)
      .map(x => {
        val appid1 = x(4)
        val appname = x(1)
        (appid1, appname)
      }).collect.toMap
    val broadcastMap = sc.broadcast(map)

    // 关键字广播出去
    val stopKeyWords: collection.Map[String, Int] = sc.textFile(stopPath).map((_, 0)).collectAsMap
    val bcstopKeyWords: Broadcast[collection.Map[String, Int]] = sc.broadcast(stopKeyWords)

    // 读取数据
    val df: DataFrame = spark.read.parquet(inputPath)
    // 过滤符合Id的数据
    val baseRDD = df.filter(TagsUtils.OneUserId).rdd
    // 接下来所有的标签都在内部实现
      .map(row=>{
      val userList = TagsUtils.getAllUserId(row)
      (userList,row)
    })
    // 构建点集合
    val vertiesRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(tp => {
      val row = tp._2
      // 所有标签
      val adList = TagsAd.makeTags(row)
      // 打上App所有标签
      val appList = TagsApp.makeTags(row, broadcastMap)
      // 打渠道标签
      val channelList = TagsChannel.makeTags(row)
      // 打关键字标签
      val keyWordsList = TagsKeyWords.makeTags(row, bcstopKeyWords)
      // 地域标签
      val areaList = TagsArea.makeTags(row)
      val buiness = TagsBusiness.makeTags(row)

      val allTag = adList ++ appList ++ channelList ++ keyWordsList ++ areaList ++ buiness

      // List((String,Int))
      // 保证其中一个点携带着所有的标签，同时也保留所有userid
      val VD = tp._1.map((_, 0)) ++ allTag
      // 处理所有的点集合
      tp._1.map(uId => {
        // 保证一个点携带标签(uid,VD),(uid,list()),(uid,list())
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })
    //vertiesRDD.take(20).foreach(println)

    // 构建边的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      // A B C:A->B,A->C
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))
    })
    // edges.take(20).foreach(println)
    // 构建图
    val graph: Graph[List[(String, Int)], Int] = Graph(vertiesRDD,edges)
    // 取出顶点, 使用的是图计算中的连通图算法
    val verties = graph.connectedComponents().vertices
    // 处理所有的标签和id
    verties.join(vertiesRDD).map{
      case (uId,(conId,tagsAll))=>(conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      (list1 ++ list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
      .take(20).foreach(println)

    spark.stop()
    sc.stop()

  }
}
