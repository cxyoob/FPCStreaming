package com.air.antispider.stream.rulecompute.launch

import com.air.antispider.stream.common.bean._
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum.TravelTypeEnum
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum, TravelTypeEnum}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.streaming.dstream.DStream

object QueryDataPackage {
  /**
    * 封装数据
    * @param lines
    */
  def queryDataLoadAndPackage(lines: DStream[String]) = {
    // 使用mapPartitions 减少包装类开销
    lines.mapPartitions(iter=>{
      // 创建JSON解析
      val mapper = new ObjectMapper
      mapper.registerModule(DefaultScalaModule)
      // 将数据进行处理
      iter.map(line=>{
        // 切分数据
        val arr = line.split("#CS#",-1)
        //原始数据，站位，并无数据
        val sourceData = arr(0)
        val requestMethod = arr(1)
        val request = arr(2)
        val remoteAddr = arr(3)
        val httpUserAgent = arr(4)
        val timeIso8601 = arr(5)
        val serverAddr = arr(6)
        val highFrqIPGroup: Boolean = arr(7).equalsIgnoreCase("true")
        val requestType: RequestType =
          RequestType(FlightTypeEnum.withName(arr(8)),
            BehaviorTypeEnum.withName(arr(9)))
        val travelType: TravelTypeEnum = TravelTypeEnum.withName(arr(10))
        val requestParams: CoreRequestParams = CoreRequestParams(arr(11), arr(12), arr(13))
        val cookieValue_JSESSIONID: String = arr(14)
        val cookieValue_USERID: String = arr(15)
        //分析查询请求的时候不需要book数据
        val bookRequestData: Option[BookRequestData] = None
        //封装query数据
        val queryRequestData = if (!arr(16).equalsIgnoreCase("NULL")) {
          mapper.readValue(arr(16), classOf[QueryRequestData]) match {
            case value if value != null => Some(value)
            case _ => None
          }
        } else {
          None
        }
        val httpReferrer = arr(18)
        // 封装流程数据,返回bean
        ProcessedData("", requestMethod, request,
          remoteAddr, httpUserAgent, timeIso8601,
          serverAddr, highFrqIPGroup,
          requestType, travelType, requestParams,
          cookieValue_JSESSIONID, cookieValue_USERID,
          queryRequestData, bookRequestData, httpReferrer)
      })
    })
  }

}
