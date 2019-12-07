package com.air.antispider.stream.dataprocess.businessprocess

import java.text.SimpleDateFormat
import java.util.Date

import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.common.util.spark.SparkMetricsUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import redis.clients.jedis.JedisCluster

object SparkStreamingMonitor {
  def queryMonitor(sc: SparkContext, messageRDD: RDD[String]): Unit = {
    //done:Spark性能实时监控
    //监控数据获取
    val sourceCount = messageRDD.count()
    // val sparkDriverHost = sc.getConf.get("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")
    //监控信息页面路径为集群路径+/proxy/+应用id+/metrics/json
    //val url = s"${sparkDriverHost}/metrics/json"
    //local模式的路径
    val url = "http://localhost:4041/metrics/json/"
    val jsonObj = SparkMetricsUtils.getMetricsJson(url)
    //应用的一些监控指标在节点gauges下
    val result = jsonObj.getJSONObject("gauges")
    //监控信息的json路径：应用id+.driver.+应用名称+具体的监控指标名称
    //最近完成批次的处理开始时间-Unix时间戳（Unix timestamp）-单位：毫秒
    val startTimePath = sc.applicationId + ".driver." + "csair_streaming_rulecompute_antispider" + ".StreamingMetrics.streaming.lastCompletedBatch_processingStartTime"
    val startValue = result.getJSONObject(startTimePath)
    var processingStartTime: Long = 0
    if (startValue != null) {
      processingStartTime = startValue.getLong("value")
    }
    //最近完成批次的处理结束时间-Unix时间戳（Unix timestamp）-单位：毫秒
    val endTimePath = sc.applicationId + ".driver." + "csair_streaming_rulecompute_antispider" + ".StreamingMetrics.streaming.lastCompletedBatch_processingEndTime"
    val endValue = result.getJSONObject(endTimePath)
    val processingEndTime = endValue.getLong("value")
    //流程所用时间
    val processTime = processingEndTime - processingStartTime
    //监控数据推送
    //done:实时处理的速度监控指标-monitorIndex需要写入Redis，由web端读取Redis并持久化到Mysql
    val endTime = processingEndTime
    val costTime = processTime
    val countPerMillis = sourceCount.toFloat / costTime.toFloat
    val monitorIndex = (endTime, "csair_streaming_rulecompute_antispider", sc.applicationId, sourceCount, costTime, countPerMillis)
    //产生不重复的key值
    val keyName = PropertiesUtil.getStringByKey("cluster.key.monitor.query", "jedisConfig.properties") + System.currentTimeMillis.toString
    val keyNameLast = PropertiesUtil.getStringByKey("cluster.key.monitor.query", "jedisConfig.properties") + "_LAST"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val processingEndTimeString = format.format(new Date(processingEndTime))
    val fieldMap = scala.collection.mutable.Map(
      "endTime" -> processingEndTimeString,
      "applicationUniqueName" -> monitorIndex._2.toString,
      "applicationId" -> monitorIndex._3.toString,
      "sourceCount" -> monitorIndex._4.toString,
      "costTime" -> monitorIndex._5.toString,
      "countPerMillis" -> monitorIndex._6.toString,
      "serversCountMap" -> Map[String, Int]())
    //将数据存入redis中
    try {
      val jedis = JedisConnectionUtil.getJedisCluster
      //监控Redis库
      //jedis.select(redis_db_monitor);
      //保存监控数据
      jedis.setex(keyName, PropertiesUtil.getStringByKey("cluster.exptime.monitor", "jedisConfig.properties").toInt, Json(DefaultFormats).write(fieldMap))
      //更新最新的监控数据
      jedis.setex(keyNameLast, PropertiesUtil.getStringByKey("cluster.exptime.monitor", "jedisConfig.properties").toInt, Json(DefaultFormats).write(fieldMap))
      //JedisConnectionUtil.returnRes(jedis)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }


  def streamMonitor(sc: SparkContext, messageRDD: RDD[String],serverCountMap: collection.Map[String, Int],jedis:JedisCluster): Unit = {
    val applicationId = sc.applicationId
    val appName = sc.appName
    //local模式的路径
    val url = "http://localhost:4040/metrics/json/"
    //获取监控json数据
    val jsonObj = SparkMetricsUtils.getMetricsJson(url)
    //应用的一些监控指标在节点gauges下
    val result = jsonObj.getJSONObject("gauges")
    //最近完成批次的处理"开始时间"-Unix时间戳（Unix timestamp）-单位：毫秒
    val startTimePath = applicationId + ".driver." + appName + ".StreamingMetrics.streaming.lastCompletedBatch_processingStartTime"
    val startValue = result.getJSONObject(startTimePath)
    var processingStartTime: Long = 0
    if (startValue != null) {
      processingStartTime = startValue.getLong("value")
    }
    //最近完成批次的处理"结束时间"-Unix时间戳（Unix timestamp）-单位：毫秒
    //local-1517307598019.driver.streaming-data-peocess.StreamingMetrics.streaming.lastCompletedBatch_processingEndTime":{"value":1517307625168}
    val endTimePath = applicationId + ".driver." + appName + ".StreamingMetrics.streaming.lastCompletedBatch_processingEndTime"
    val endValue = result.getJSONObject(endTimePath)
    var processingEndTime: Long = 0
    if (endValue != null) {
      processingEndTime = endValue.getLong("value")
    }
    //最近批处理的数据行数
    val sourceCount = messageRDD.count()
    //最近批处理所花费的时间
    val costTime = processingEndTime - processingStartTime
    //监控指标（平均计算时间）:实时处理的速度监控指标-monitorIndex需要写入Redis，由web端读取Redis并持久化到Mysql
    val countPerMillis = sourceCount.toFloat / costTime.toFloat
    //将数据封装到map中
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val processingEndTimeString = format.format(new Date(processingEndTime))
    val fieldMap = scala.collection.mutable.Map(
      "endTime" -> processingEndTimeString,
      "applicationUniqueName" -> appName.toString,
      "applicationId" -> applicationId.toString,
      "sourceCount" -> sourceCount.toString,
      "costTime" -> costTime.toString,
      "countPerMillis" -> countPerMillis.toString,
      "serversCountMap" -> serverCountMap)
    /*
      *存储监控数据到redis
     */
    try {
      //监控记录有效期，单位秒
      val monitorDataExpTime: Int = PropertiesUtil.getStringByKey("cluster.exptime.monitor", "jedisConfig.properties").toInt
      //产生不重复的key值
      val keyName = PropertiesUtil.getStringByKey("cluster.key.monitor.dataProcess", "jedisConfig.properties") + System.currentTimeMillis.toString
      val keyNameLast = PropertiesUtil.getStringByKey("cluster.key.monitor.dataProcess", "jedisConfig.properties") + "_LAST"
      //保存监控数据
      jedis.setex(keyName, monitorDataExpTime, Json(DefaultFormats).write(fieldMap))
      //更新最新的监控数据
      jedis.setex(keyNameLast, monitorDataExpTime, Json(DefaultFormats).write(fieldMap))
      //JedisConnectionUtil.returnRes(jedis)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

}
