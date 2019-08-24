package com.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

/**
  *  商圈解析工具
  */
object AliMapUtil {

  // 获取高德地图商圈信息
  def getBusinessFromAliMap(long:Double,lat:Double):String={
    val location = long + "," + lat
    //https://restapi.amap.com/v3/geocode/regeo?location="+location+"
    // &key=99268cb756f0d653bf425c3e187da271&radius=1000
    val urlStr = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=e925945032d649fb0c60bdcb39d5de50&radius=1000"
    val jsonStr: String = HttpUtil.get(urlStr)
    // 调用请求
    // 解析json串
    val jsonParse: JSONObject = JSON.parseObject(jsonStr)
    // 判断状态是否成功
    val status = jsonParse.getIntValue("status")
    if(status == 0) return ""
    // 接下来解析内部json串，判断每个key的value都不能为空
    val redeocodejson: JSONObject = jsonParse.getJSONObject("regeocode")
    if(redeocodejson == null || redeocodejson.keySet().isEmpty)return ""
    val addressComponentJson: JSONObject = redeocodejson.getJSONObject("addressComponent")
   if(addressComponentJson == null || addressComponentJson.keySet().isEmpty)return ""
    val businessAreasArray: JSONArray = addressComponentJson.getJSONArray("businessAreas")
   if(businessAreasArray == null || businessAreasArray.isEmpty)return  null
    // 创建集合，保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    // 循环输出
    for(item <- businessAreasArray.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")
  }
}
