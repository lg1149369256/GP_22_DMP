package test1

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.sql. SparkSession

object TagsTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val log = spark.read.textFile("D://qianfeng/gp22Dmp/zhoukao/json.txt").rdd.collect.toBuffer

    // 创建list接收数据
    var list: List[String] = List()
    for(i <- 0 to log.length -1) {
      val str: String = log(i).toString
      val jsonparse: JSONObject = JSON.parseObject(str)
      val status = jsonparse.getIntValue("status")
      if (status == 0) return ""
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""
      val poisArray = regeocodeJson.getJSONArray("pois")
      if (poisArray == null || poisArray.isEmpty) return null
      // 创建集合 保存数据
      val buffer = collection.mutable.ListBuffer[String]()
      // 循环输出
      for (item <- poisArray.toArray) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("type"))
        }
      }
      list:+=buffer.mkString(",")
    }
    val res: List[(String, Int)] = list.flatMap(x => x.split(","))
      .flatMap(x=>x.split(";"))
      .map((_,1)).groupBy(_._1).mapValues(_.size).toList

    res.foreach(println)

  }
}
