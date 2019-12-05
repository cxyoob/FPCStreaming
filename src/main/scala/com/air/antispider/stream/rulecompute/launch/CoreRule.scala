package com.air.antispider.stream.rulecompute.launch

import com.air.antispider.stream.common.bean.ProcessedData
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer

object CoreRule {
  def criticalCookies(structuredDataLines: DStream[ProcessedData],
                      windowDuration: Duration,
                      slideDuration: Duration,
                      queryCriticalPages: ArrayBuffer[String]): DStream[(String, Iterable[String])] = {
    structuredDataLines.map { processedData =>
      val request = processedData.request
      var flag = false
      for (i <- 0 until queryCriticalPages.size) {
        if (request.matches(queryCriticalPages(i))) {
          flag = true
        }
      }
      if (flag) {
        (processedData.remoteAddr, processedData.cookieValue_JSESSIONID)
      } else {
        (processedData.remoteAddr, "")
      }
    }.groupByKeyAndWindow(windowDuration, slideDuration)
  }


  def flightQuerys(structuredDataLines: DStream[ProcessedData],
                   windowDuration: Duration,
                   slideDuration: Duration): DStream[(String, Iterable[(String, String)])] = {
    structuredDataLines.map { processedData =>
      (processedData.remoteAddr, (processedData.requestParams.depcity, processedData.requestParams.arrcity))
    }.groupByKeyAndWindow(windowDuration, slideDuration)
  }

  def aCriticalPagesAccTime(structuredDataLines: DStream[ProcessedData],
                            windowDuration: Duration,
                            slideDuration: Duration,
                            queryCriticalPages: ArrayBuffer[String]): DStream[((String, String), Iterable[String])] = {
    structuredDataLines.map { processedData =>
      val accessTime = processedData.timeIso8601
      val request = processedData.request
      var flag = false
      for (i <- 0 until queryCriticalPages.size) {
        if (request.matches(queryCriticalPages(i))) {
          flag = true
        }
      }
      if (flag) {
        ((processedData.remoteAddr, request), accessTime)
      } else {
        ((processedData.remoteAddr, request), "0")
      }
    }.groupByKeyAndWindow(windowDuration, slideDuration)
  }

  def criticalPagesAccTime(structuredDataLines: DStream[ProcessedData],
                           windowDuration: Duration,
                           slideDuration: Duration,
                           queryCriticalPages: ArrayBuffer[String]): DStream[(String, Iterable[String])] = {

    structuredDataLines.map { processedData =>
      val accessTime = processedData.timeIso8601
      val request = processedData.request
      var flag = false
      for (i <- 0 until queryCriticalPages.size) {
        if (request.matches(queryCriticalPages(i))) {
          flag = true
        }
      }
      if (flag) {
        (processedData.remoteAddr, accessTime)
      } else {
        (processedData.remoteAddr, "0")
      }
    }.groupByKeyAndWindow(windowDuration, slideDuration)
  }

  def userAgent(structuredDataLines: DStream[ProcessedData],
                windowDuration: Duration,
                slideDuration: Duration): DStream[(String, Iterable[String])] = {
    structuredDataLines.map { processedData =>
      (processedData.remoteAddr, processedData.httpUserAgent)
    }.groupByKeyAndWindow(windowDuration, slideDuration)
  }

  def criticalPagesCounts(structuredDataLines: DStream[ProcessedData],
                          windowDuration: Duration,
                          slideDuration: Duration, queryCriticalPages: ArrayBuffer[String]): DStream[(String, Int)] = {
    structuredDataLines.map { processedData =>
      //从request中拿到访问页面
      val request = processedData.request
      //判断页面是否为关键页面
      var flag = false
      for (page <- queryCriticalPages) {
        if (request.matches(page)) {
          flag = true
        }
      }
      //如果是关键页面，记录这个ip为1
      if (flag) {
        (processedData.remoteAddr, 1)
      } else {
        //如果不是关键页面，记录为0
        (processedData.remoteAddr, 0)
      }
      //统计
    }.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), windowDuration, slideDuration)
  }

  def ipAccessCounts(structuredDataLines: DStream[ProcessedData],
                     windowDuration: Duration,
                     slideDuration: Duration): DStream[(String, Int)] = {

    structuredDataLines.map { processedData =>
      (processedData.remoteAddr, 1)
    }.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), windowDuration, slideDuration)
  }



  def ipBlockAccessCounts(structuredDataLines: DStream[ProcessedData],
                          windowDuration: Duration,
                          slideDuration: Duration): DStream[(String, Int)] = {

    structuredDataLines.map { processedData =>
      //remoteaddr没数据
      if ("NULL".equalsIgnoreCase(processedData.remoteAddr)) {
        (processedData.remoteAddr, 1)
      } else {
        //remoteaddr有数据,如192.168.56.151第一个“.”出现的位置
        val index = processedData.remoteAddr.indexOf(".")
        try {
          (processedData.remoteAddr.substring(0, processedData.remoteAddr.indexOf(".", index + 1)), 1)
        } catch {
          case e: Exception =>
            ("", 1)
        }
      }
      //叠加5分钟的数据
    }.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), windowDuration, slideDuration)

  }

}
