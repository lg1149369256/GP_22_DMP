package com.tags

import com.utils.Tags
import org.apache.spark.sql.Row

object TagsShebei extends Tags{
  /**
    * 打标签的统一接口
    */
  override def makeTags (args: Any*): List[(String, Int)] = {
    var list = List[(String ,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]

    // 设备操作系统
    val cli = row.getAs[Int ]("client")
    cli match {
      case 1 => list:+=("Android D00010001",1)
      case 2 =>  list:+=("IOS D00010002",1)
      case 3 =>  list:+=("WinPhone  D00010003",1)
      case _ =>  list:+=("其他 D00010004",1)
    }
    // 设 备 联 网 方 式
    val networkmannername = row.getAs[String]("networkmannername")
    networkmannername match {
      case "WIFI" => list:+=("D00020001 ",1)
      case "4G" => list:+=("D00020001 ",1)
      case "3G" => list:+=("D00020001 ",1)
      case "2G" => list:+=("D00020001 ",1)
      case  _  => list:+=("D00020001 ",1)
    }
    // 设备运营商方式
    val ispName = row.getAs[String ]("ispname")
    ispName match {
      case "移动" => list:+=("D00030001",1)
      case "联通" => list:+=("D00030002",1)
      case "电信" => list:+=("D00030003",1)
      case _ => list:+=("D000300014",1)
    }
  list
  }
}
