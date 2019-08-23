package com.tags

import com.utils.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object TagsChannel extends Tags{
  /**
    * 打标签的统一接口
    */
  override def makeTags (args: Any*): List[(String, Int)] = {
    var list = List[(String ,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]

    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")

    if(StringUtils.isNotBlank("adplatformproviderid")){
      list :+= ("CN" + adplatformproviderid,1)
    }
    list
  }
}
