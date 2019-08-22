package com.utils

/**
  * 指标方法
  */
object RptUtils {

  // 此方法处理请求数
  //  原始请求数，有效请求，广告请求
  def request(requestmode:Int,processnode:Int):List[Double]={
    if(requestmode == 1 && processnode == 1){
      List[Double](1,0,0)
    }else if (requestmode == 1 && processnode == 2){
      List[Double](1,1,0)
    }else if(requestmode == 1 && processnode == 3){
      List[Double](1,1,1)
    } else {
      List[Double](0,0,0)
    }
  }

  // 此方法处理展示点击数
  // 展示数，点击数
  def click(requestmode:Int,iseffective:Int):List[Double]={
    if(requestmode == 2 && iseffective == 1){
      List[Double](1,0)
    }else if(requestmode == 3 && iseffective == 1){
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }
  }
  // 此方法处理竞价操作
  //参与竞价数，竞价成功数，广告消费 ，广告成本
  def Ad(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,
         adorderid:Int,WinPrice:Double,adpayment:Double):List[Double]={
    if(iseffective == 1 && isbilling == 1 && isbid == 1){
      List[Double](1,0,0,0)
    }else if(iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0){
      List[Double](0,1,0,0)
    }else if(iseffective == 1 && isbilling == 1 && iswin == 1 ){
      List[Double](0,0,WinPrice/1000.0,adpayment/1000.0)
    }else{
      List[Double](0,0,0,0)
    }
  }
}
