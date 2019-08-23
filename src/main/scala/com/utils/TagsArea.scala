package com.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsArea extends Tags{
  /**
    * 打标签的统一接口
    */
  override def makeTags (args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 解析参数
    val row =args(0).asInstanceOf[Row]

    val pro = row.getAs[String]("provincename")
    val city = row.getAs[String]("cityname")
    if(StringUtils.isNotBlank(pro)){
      list:+=("ZP" + pro,1)
    }
    if(StringUtils.isNotBlank(city)){
      list:+=("ZC" + city,1)
    }
    list
  }
}
