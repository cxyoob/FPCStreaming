package com.air.antispider.stream.dataprocess.businessprocess

import scala.collection.mutable.ArrayBuffer

object IpOperation {


  def isFreIP(ip: String, ipFileds: ArrayBuffer[String]): Boolean = {

    //初始化标记
    var resultFlag = false

    //mysql的ip黑名单已经存储在广播变量中，循环广播变量的黑名单数据

    val it = ipFileds.iterator

    while (it.hasNext && !resultFlag) {

      val mysqlTemp = it.next()

      //匹配

      if (mysqlTemp.equals(ip)) {

        //做标记

        resultFlag = true

      }

    }

    //返回标记

    resultFlag

  }


}
