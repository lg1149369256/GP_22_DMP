package com.tags

import com.utils.{AliMapUtil, Tags}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsBuiness extends Tags{
  /**
    * 打标签的统一接口
    */
  override def makeTags (args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]

    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")

    if(StringUtils.isNotBlank(long)&& StringUtils.isNotBlank(lat)) {
      val str: String = AliMapUtil.getBusinessFromAliMap(long.toDouble, lat.toDouble)
      list:+=(str,1)
    }
    list
  }
}
