package com.tags



import com.typesafe.config.ConfigFactory
import com.utils.{TagsArea, TagsUtils}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD


/**
  *  上下文标签
  */
object TagsContext {
  def main (args: Array[String]): Unit = {
    if(args.length != 4){
      println("目录不正确，退出程序！！！")
      sys.exit()
    }
    val Array(inputPath,inputPath2,stopPath,days) = args
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
      val buiness = TagsBusiness.makeTags(row)
      (userId, adList ++ appList ++ channelList ++ keyWordsList ++ areaList++buiness)
    })
    val aggrUserTags = tups.reduceByKey((list1,list2) => {
      // List(("LN插屏",1),("LN全屏",1),("ZP河南",1),("ZC商丘",1)...)
      (list1 ::: list2)
        .groupBy(_._1).mapValues(x => x.foldLeft(0)((x, y) => (x + y._2))).toList
    }).map{
      case(userid,userTag)=>{
        val put = new Put(Bytes.toBytes(userid))

        // 处理下标签
        val tags = userTag.map(t=> t._1 + "," + t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(days),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }}
      // 保存到对应的表中
      .saveAsHadoopDataset(jobconf)


    spark.stop()
    sc.stop()

  }
}
