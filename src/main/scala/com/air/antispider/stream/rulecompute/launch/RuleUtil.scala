package com.air.antispider.stream.rulecompute.launch


import java.text.SimpleDateFormat
import java.util

import com.air.antispider.stream.common.bean.{FlowCollocation, ProcessedData}
import com.air.antispider.stream.rulecompute.businessprocess.{AntiCalculateResult, FlowScoreResult}
import org.joda.time.DateTime

import scala.collection.mutable


object RuleUtil {

  def calculateFlowsScore(paramMap: mutable.Map[String, Int], FlowCollocations: Array[FlowCollocation]): Array[FlowScoreResult] = ???

  def calculateAntiResult(processedData: ProcessedData,
                          FlowCollocations: Array[FlowCollocation],
                          ip: String, request: String,
                          ipBlockAccessCountsMap: scala.collection.Map[String, Int],
                          ipAccessCountsMap: scala.collection.Map[String, Int],
                          criticalPagesCountsMap: scala.collection.Map[String, Int],
                          userAgentCountsMap: scala.collection.Map[String, Int],
                          criticalMinIntervalMap: scala.collection.Map[String, Int],
                          accessIntervalLessThanDefaultMap: scala.collection.Map[(String, String), Int],
                          differentTripQuerysMap: scala.collection.Map[String, Int],
                          criticalCookiesMap: scala.collection.Map[String, Int],
                         ): AntiCalculateResult = {
    //当前处理这个ip的段
    val index = ip.indexOf(".")
    val ipBlock = try {
      ip.substring(0, ip.indexOf(".", index + 1))
    } catch {
      case e: Exception => ""
    }
    //IP段访问量
    val ipBlockCounts = ipBlockAccessCountsMap.getOrElse(ipBlock, 0)
    //这条记录对应的单位时间访问量
    val ipAccessCounts = ipAccessCountsMap.getOrElse(ip, 0)
    //这条记录对应的单位时间内的关键页面访问总量
    val criticalPageAccessCounts = criticalPagesCountsMap.getOrElse(ip, 0)
    //这条记录对应的单位时间内的UA种类数统计
    val userAgentCounts = userAgentCountsMap.getOrElse(ip, 0)
    //这条记录对应的单位时间内的关键页面最短访问间隔
    val critivalPageMinInterval = criticalMinIntervalMap.getOrElse(ip, 0)
    //这条记录对应的单位时间内小于最短访问间隔（自设）的关键页面查询次数
    val accessPageIntervalLessThanDefault = accessIntervalLessThanDefaultMap.getOrElse((ip, request), 0)
    //这条记录对应的单位时间内查询不同行程的次数
    val differentTripQuerysCounts = differentTripQuerysMap.getOrElse(ip, 0)
    //这条记录对应的单位时间内关键页面的Cookie数
    val criticalCookies = criticalCookiesMap.getOrElse(ip, 0)
    //这条记录对应的所有标签封装到map中
    val paramMap = scala.collection.mutable.Map[String, Int]()
    paramMap += ("ipBlock" -> ipBlockCounts)
    paramMap += ("ip" -> ipAccessCounts)
    paramMap += ("criticalPages" -> criticalPageAccessCounts)
    paramMap += ("userAgent" -> userAgentCounts)
    paramMap += ("criticalPagesAccTime" -> critivalPageMinInterval)
    paramMap += ("flightQuery" -> differentTripQuerysCounts)
    paramMap += ("criticalCookies" -> criticalCookies)
    paramMap += ("criticalPagesLessThanDefault" -> accessPageIntervalLessThanDefault)
    val flowsScore: Array[FlowScoreResult] = calculateFlowsScore(paramMap, FlowCollocations)
    //针对这条记录封装的打分类，包含了这条记录的所有统计结果、打分、是否命中等等
    AntiCalculateResult(processedData, ip, ipBlockCounts, ipAccessCounts,
      criticalPageAccessCounts, userAgentCounts, critivalPageMinInterval,
      accessPageIntervalLessThanDefault, differentTripQuerysCounts,
      criticalCookies, flowsScore)
  }

  def cookiesCounts(cookies: Iterable[String]): Int = {
    val list: java.util.List[String] = new java.util.ArrayList[String]
    for (cookie <- cookies) {
      if (!"".equals(cookie)) {
        list.add(cookie)
      }
    }
    val hashSet = new util.HashSet(list)
    list.clear()
    list.addAll(hashSet)
    list.size()
  }


  def calculateDifferentTripQuerys(querys: Iterable[(String, String)]): Int = {
    val list: java.util.List[String] = new java.util.ArrayList[String]()
    for (query <- querys) {
      list.add(query._1 + "-->" + query._2)
    }
    val hashSet = new util.HashSet(list)
    list.clear()
    list.addAll(hashSet)
    list.size()
  }


  def intervalsLessThanDefault(intervals: Iterable[String]): Int = {
    // 预设时间间隔 10秒
    val defaultMinInterval = 10
    var count = 0
    val allTime = calculateIntervals(intervals)
    val interval = allInterval(allTime)
    if(interval!=null && interval.size()>0){
      for (i<- 0 until interval.size()){
        if(interval.get(i)<defaultMinInterval){
          count = count+1
        }
      }
    }
    count
  }


  def allInterval(list: util.List[Long]) = {
    // 排序：因为获取的数据，不能保证顺序性
    val arr = list.toArray
    util.Arrays.sort(arr)
    // 创建list用于封装数据
    val intervalList = new util.ArrayList[Long]()
    // 计算时间差
    if(arr.length>1){
      for (i<-1 until arr.length){
        val time1 = arr(i-1).toString.toLong
        val time2 = arr(i).toString.toLong
        val interval  = time2- time1
        intervalList.add(interval)
      }
    }
    intervalList
  }

  def minInterval(intervals: java.util.List[Long]): Int = {
    // 计算时间间隔
    val intervalLsit = allInterval(intervals)
    // 排序
    val result = intervalLsit.toArray()
    util.Arrays.sort(result)
    result(0).toString.toInt
  }


  def calculateIntervals(accTimes: Iterable[String]):java.util.List[Long] = {
    val timeList = new util.ArrayList[Long]()
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    for(time<- accTimes){
      if(!"".equalsIgnoreCase(time)){
        val timeStr = new DateTime(time).toDate
        val dt = sdf.format(timeStr)
        val timeL = sdf.parse(dt).getTime
      }
    }
    timeList
  }
  def differentAgents(agents: Iterable[String]): Int = {
    val set = mutable.Set[String]()
    for (agent <- agents) {
      set.add(agent)
    }
    set.size

  }

}
