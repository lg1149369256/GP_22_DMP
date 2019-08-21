package com.utils


/**
  * 数据类型转换
  */
object Utils2Type {
  // String转换Int
    def toInt(str:String):Int= {
        try {
          str.toInt
        } catch {
          case ex:Exception => 0
        }
    }

    // String 转 Double
    def toDouble(str:String)={
        try {
          str.toDouble
        } catch {
          case ex:Exception => 0.0
        }
    }
}
