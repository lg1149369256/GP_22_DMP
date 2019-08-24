package test1

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object TestCount {
  def main (args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    var list: List[List[String]] = List()
    val logs = spark.read.textFile("D://qianfeng/gp22Dmp/zhoukao/json.txt").rdd.collect.toBuffer
    for(i <- 0 to logs.length -1) {
      val str: String = logs(i).toString
      // 解析json串
      val jsonParse: JSONObject = JSON.parseObject(str)
      // 判断状态是否成功
      val status = jsonParse.getIntValue("status")
      if (status == 0) return ""
      // 接下来解析内部json串，判断每个key的value都不能为空
      val redeocodejson: JSONObject = jsonParse.getJSONObject("regeocode")
      if (redeocodejson == null || redeocodejson.keySet().isEmpty) return ""

      val businessAreasArray: JSONArray = redeocodejson.getJSONArray("pois")
      if (businessAreasArray == null || businessAreasArray.isEmpty) return null
      // 创建集合，保存数据
      val buffer = collection.mutable.ListBuffer[(String)]()
      // 循环输出
      for (item <- businessAreasArray.toArray) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("businessarea"))
        }
      }
      list :+= buffer.toList
    }
      val res: List[(String, Int)] = list.flatMap(x => x).filter(x=> !x.contains("[]")).map((_,1))
        .groupBy(_._1).mapValues(x => x.size).toList

      res.foreach(println)
    }
  }
