package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.AccessLog
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer

object UrlFilter {
  def filterUrl(log: AccessLog, filterRuleRef: Broadcast[ArrayBuffer[String]]): Boolean = {
    var isMatch = true
    filterRuleRef.value.foreach(str=>{
      // 匹配URL
      if(log.request.matches(str)){
        isMatch = false
      }
    })
    isMatch
  }

}
