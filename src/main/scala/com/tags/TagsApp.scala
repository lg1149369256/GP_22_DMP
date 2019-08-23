package com.tags

import com.utils.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsApp extends Tags{
  /**
    * 打标签的统一接口
    */
  override def makeTags (args: Any*): List[(String, Int)] = {
    var list =  List[(String,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    val broadcast1 = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    var appname = row.getAs[String]("appname")
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP" + appname,1)
    }
    val appid = row.getAs[String]("appid")
    if(StringUtils.isNotBlank(appid)){
      appname = broadcast1.value.getOrElse(appid, "NoKnow")
      list:+=("APP" + appname,1)
    }
    list
  }
}