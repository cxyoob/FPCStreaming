package com.air.antispider.stream.dataprocess.launch

import java.util

import com.air.antispider.stream.common.bean.{AccessLog, AnalyzeRule, QueryRequestData, RequestType}
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.dataprocess.businessprocess.{AnalyzeBookRequest, AnalyzeRequest, AnalyzeRuleDB, BroadcastProcess, BusinessProcess, DataPackage, DataSend, DataSplit, EntcryedData, IpOperation, RequestTypeClassifier, SparkStreamingMonitor, TravelTypeClassifier, UrlFilter}
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum.TravelTypeEnum
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

object DataProcessLauncher {

  def setupSsc(sc: SparkContext,
               sql: SQLContext,
               kafkaParams: Map[String, Object],
               topics: Set[String]) = {
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(3))
    val stream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    val filterRulelist = AnalyzeRuleDB.queryFilterRule()
    @volatile var filterRuleRef: Broadcast[ArrayBuffer[String]] = sc.broadcast(filterRulelist)
    val ruleMap: util.Map[String, ArrayBuffer[String]] = AnalyzeRuleDB.queryRuleMap()
    @volatile var broadcastRuleMap: Broadcast[util.Map[String, ArrayBuffer[String]]] = sc.broadcast(ruleMap)
    val queryRulesList: List[AnalyzeRule] = AnalyzeRuleDB.queryQueryRules()
    @volatile var broadcastQueryRulesList: Broadcast[List[AnalyzeRule]] = sc.broadcast(queryRulesList)
    val ipBlackList = AnalyzeRuleDB.queryIpBlackList()
    @volatile var broadcastIPList = sc.broadcast(ipBlackList)
    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster
    stream.map(_.value()).foreachRDD(rdd=>{
      rdd.cache()
      filterRuleRef = BroadcastProcess.BroadFilterRule(sc,filterRuleRef,jedis)
      broadcastRuleMap = BroadcastProcess.BroadRuleMap(sc,broadcastRuleMap,jedis)
      broadcastQueryRulesList = BroadcastProcess.BroadQueryRule(sc,broadcastQueryRulesList,jedis)
      broadcastIPList = BroadcastProcess.mointorBlackListRule(sc,broadcastIPList,jedis)
      val value:RDD[AccessLog] = DataSplit.parseAccessLog(rdd)
      val serversCount= BusinessProcess.linkCount(value,jedis)
      val serverCountMap = serversCount.collectAsMap()
      val filterRdd: RDD[AccessLog] = value.filter(log=>UrlFilter.filterUrl(log,filterRuleRef))
      val processedDataRdd = filterRdd.map(
        accessLog => {
          accessLog.http_cookie = EntcryedData.encryptedPhone(accessLog.http_cookie)
          accessLog.http_cookie = EntcryedData.encryptedId(accessLog.http_cookie)
          val requestTypeLabel: RequestType = RequestTypeClassifier.classifyByRequest(accessLog.request, broadcastRuleMap.value)
          val travelType: TravelTypeEnum = TravelTypeClassifier.classifyByRefererAndRequestBody(requestTypeLabel, accessLog.http_referer, accessLog.request_body)
          val queryRequestData = AnalyzeRequest.analyzeQueryRequest( requestTypeLabel, accessLog.request_method, accessLog.content_type, accessLog.request, accessLog.request_body, travelType, broadcastQueryRulesList.value)
          val bookRequestData = AnalyzeBookRequest.analyzeBookRequest( requestTypeLabel, accessLog.request_method, accessLog.content_type, accessLog.request, accessLog.request_body, travelType, broadcastQueryRulesList.value)
          val highFrqIPGroup = IpOperation.isFreIP(accessLog.remote_addr, broadcastIPList.value)
          val processedData = DataPackage.dataPackage("",
            accessLog.request_method, accessLog.request, accessLog.remote_addr,
            accessLog.http_user_agent, accessLog.time_iso8601, accessLog.server_addr,
            highFrqIPGroup, requestTypeLabel, travelType, accessLog.jessionID,
            accessLog.userID, queryRequestData, bookRequestData, accessLog.http_referer)
          processedData
        })
      //查询行为数据 for Kafka
      DataSend.sendQueryDataToKafka(processedDataRdd)
      //预订行为数据 for Kafka
//      DataSend.sendBookDataToKafka(processedDataRdd)
      SparkStreamingMonitor.streamMonitor(sc, rdd, serverCountMap,jedis)
      println("完成")
    })
    ssc
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")
    val conf: SparkConf = new SparkConf().setAppName("Streaming-1").setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = new SparkContext(conf)
    val sql: SQLContext = new SQLContext(sc)
    val groupId = "b2c1901"
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      // 设置从头消费
      "auto.offset.reset" -> "earliest"
    )
    val topics = Set(PropertiesUtil
      .getStringByKey("source.nginx.topic","kafkaConfig.properties"))
    val ssc = setupSsc(sc,sql,kafkaParams,topics)

    ssc.start()
    ssc.awaitTermination()

  }
}
