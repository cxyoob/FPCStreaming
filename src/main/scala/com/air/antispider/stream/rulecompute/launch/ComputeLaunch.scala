package com.air.antispider.stream.rulecompute.launch


import java.text.SimpleDateFormat

import com.air.antispider.stream.common.bean.ProcessedData
import com.air.antispider.stream.common.util.hdfs.BlackListToRedis
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.common.util.kafka.KafkaOffsetUtil
import com.air.antispider.stream.common.util.log4j.LoggerLevels
import com.air.antispider.stream.dataprocess.businessprocess.AnalyzeRuleDB
import com.air.antispider.stream.rulecompute.businessprocess.{BroadcastProcess, FlowScoreResult}
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.codec.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 实时统计主类
  */
object ComputeLaunch {

  def main(args: Array[String]): Unit = {

    //设置日志级别
    LoggerLevels.setStreamingLogLevels()
    //当应用被停止的时候，进行如下设置可以保证当前批次执行完之后再停止应用。
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")
    //创建sparkConf
    val conf = new SparkConf().setAppName("csair_streaming_rulecompute_antispider") .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //创建sparkContext
    val sc = new SparkContext(conf)
    //创建sqlcontext
    val sqlContext = new SQLContext(sc)
    val groupId = "b2c1901"
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      // 设置从头消费
      "auto.offset.reset" -> "earliest"
    )
    //dataprocess生产的数据topic
    val sourceQueryTopic: Set[String] = Set(PropertiesUtil.getStringByKey("source.query.topic", "kafkaConfig.properties"))
    val zkHosts = PropertiesUtil.getStringByKey("zkHosts", "zookeeperConfig.properties")
    val zkPath = PropertiesUtil.getStringByKey("rulecompute.antispider.zkPath", "zookeeperConfig.properties")
    val zkClient = new ZkClient(zkHosts, 30000, 30000)
    //调用业务处理方法
    val ssc = setupSsc(sc, sqlContext, sourceQueryTopic, kafkaParams, zkClient, zkHosts, zkPath)
    //启动streaming程序
    ssc.start()
    ssc.awaitTermination()
  }

  def createCustomDirectKafkaStream(ssc: StreamingContext,
                                    kafkaParams: Map[String, Object],
                                    zkClient: ZkClient,
                                    zkHosts: String,
                                    zkPath: String,
                                    topics: Set[String]): InputDStream[ConsumerRecord[String, String]] = {
    //拿出topic
    val topic = topics.last
    //读取保存的offser
    val storedOffsets = KafkaOffsetUtil.readOffsets(zkClient, zkHosts, zkPath, topic)
    //拿到offset去匹配
    val kafkaStream = storedOffsets match {
      case None => //如果没有数据，就从这个topic保存在zk上的offset开始读取数据
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )
      case Some(fromOffsets) => // 如果有数据，就从记录的offset读取数据，避免数据丢失
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )
    }
    //返回kafka数据流
    kafkaStream
  }

  def setupSsc(sc: SparkContext,
               sqlContext: SQLContext,
               topics: Set[String],
               kafkaParams: Map[String, Object],
               zkClient: ZkClient, zkHosts: String,
               zkPath: String
              ): StreamingContext = {
    // 创建streaming
    val ssc = new StreamingContext(sc, Seconds(3))
    val queryCriticalPages = AnalyzeRuleDB.queryCriticalPages()
    @volatile var broadcastQueryCriticalPages = sc.broadcast(queryCriticalPages)
    val ipInitList = AnalyzeRuleDB.queryIpListToBrocast()
    @volatile var broadcastIPList = sc.broadcast(ipInitList)
    val flowList = AnalyzeRuleDB.createFlow(0)
    @volatile var broadcastFlowList = sc.broadcast(flowList)
    // 消费kafka数据
    val stream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    val messages = createCustomDirectKafkaStream(ssc, kafkaParams, zkClient, zkHosts, zkPath, topics)
//    println("测试")
//    messages.print()
    //保存offset到zookeeper
    messages.foreachRDD(rdd => KafkaOffsetUtil.saveOffsets(zkClient, zkHosts, zkPath, rdd))
    val jedis = JedisConnectionUtil.getJedisCluster
    // 数据处理
    val lines = stream.map(_.value())
    // todo 数据封装
    val structuredDataLines =QueryDataPackage.queryDataLoadAndPackage(lines)
    lines.foreachRDD(rdd=>{
      // 更新关键页面
      broadcastQueryCriticalPages =
        BroadcastProcess.monitorQueryCriticalPagesRule(sc,broadcastQueryCriticalPages,jedis)
      // 更新黑名单
      broadcastIPList =
        BroadcastProcess.monitorIpBlackListRule(sc,broadcastIPList,jedis)
      // 更新流程规则
      broadcastFlowList =
        BroadcastProcess.monitorFlowListRule(sc,broadcastFlowList,jedis)
    })

    val ipBlock = CoreRule.ipBlockAccessCounts(structuredDataLines, Minutes(5), Seconds(30))
    //IP访问数据统计
    var ipBlockAccessCountsMap  = collection.Map[String, Int]()
    ipBlock.foreachRDD { rdd =>
      ipBlockAccessCountsMap = rdd.collectAsMap()
    }
    //测试
//    ipBlock.foreachRDD{ x =>x.foreach(println)}
    val ip = CoreRule.ipAccessCounts(structuredDataLines, Minutes(5), Seconds(30))
    var ipAccessCountsMap = collection.Map[String, Int]()
    ip.foreachRDD { r =>
      ipAccessCountsMap = r.collectAsMap()
    }
    val criticalPages = CoreRule.criticalPagesCounts(structuredDataLines, Minutes(5), Seconds(30), broadcastQueryCriticalPages.value)

    var criticalPagesCountsMap = scala.collection.Map[String, Int]()
    criticalPages.foreachRDD { criticalPageRdd =>
      val stringToInt: collection.Map[String, Int] = criticalPageRdd.collectAsMap()
      criticalPagesCountsMap = criticalPageRdd.collectAsMap()
    }
    //测试
//    criticalPages.foreachRDD(x =>x.foreach(println))
    val userAgent = CoreRule.userAgent(structuredDataLines, Minutes(5), Seconds(30))
    var userAgentCountsMap = scala.collection.Map[String, Int]()
    userAgent.map { userAgents =>
      val agents = userAgents._2
      (userAgents._1, RuleUtil.differentAgents(agents))
    }.foreachRDD { userAgentRdd =>
      userAgentCountsMap = userAgentRdd.collectAsMap()
    }
    //测试
//    userAgent.foreachRDD(x => x.foreach(println))

    val criticalPagesAccTime = CoreRule.criticalPagesAccTime(structuredDataLines, Seconds(30), Seconds(30), broadcastQueryCriticalPages.value)
    var criticalMinIntervalMap = scala.collection.Map[String, Int]()
    criticalPagesAccTime.map { record =>
      val accTimes = record._2
      //计算时间差
      val intervals = RuleUtil.calculateIntervals(accTimes)
      //将remoteaddr和时间差最小值封装到tuple
      (record._1, RuleUtil.minInterval(intervals))
    }.foreachRDD { critivalMinIntervalRdd =>
      //收集数据
      criticalMinIntervalMap = critivalMinIntervalRdd.collectAsMap()
    }
    //测试
//    criticalPagesAccTime.foreachRDD(x => x.foreach(println))

    val aCriticalPagesAccTime = CoreRule.aCriticalPagesAccTime(
      structuredDataLines, Seconds(9), Seconds(3), broadcastQueryCriticalPages.value)
    var acriticalPagesTimeMap = scala.collection.Map[(String,String),Int]()
    aCriticalPagesAccTime.map(rdd=>{
      val accTime = rdd._2
      val count = RuleUtil.intervalsLessThanDefault(accTime)
      (rdd._1,count)
    }).foreachRDD(rdd=>{
      acriticalPagesTimeMap = rdd.collectAsMap()
    })
    //测试
//    aCriticalPagesAccTime.foreachRDD(x => x.foreach(println))

    val flightQuery = CoreRule.flightQuerys(structuredDataLines, Minutes(5), Seconds(30))
    //统计一个IP下查询不同行程的次数
    var differentTripQuerysMap = scala.collection.Map[String, Int]()
    flightQuery.map { record =>
      val querys = record._2
      (record._1, RuleUtil.calculateDifferentTripQuerys(querys))
    }.foreachRDD { rdd =>
      differentTripQuerysMap = rdd.collectAsMap()
      //测试
      println(differentTripQuerysMap)
    }
    //测试
//    flightQuery.foreachRDD(x => x.foreach(println))

    val criticalCookie = CoreRule.criticalCookies(structuredDataLines, Minutes(5), Seconds(30), broadcastQueryCriticalPages.value)
    //统计一个IP下不同Cookie数
    var criticalCookiesMap = scala.collection.Map[String, Int]()
    criticalCookie.map { record =>
      val cookies = record._2
      (record._1, RuleUtil.cookiesCounts(cookies))
    }.foreachRDD { rdd =>
      criticalCookiesMap = rdd.collectAsMap()
    }
    //测试
//    criticalCookie.foreachRDD(x => x.foreach(println))

    val antiCalculateResults = structuredDataLines.map { processedData =>
      //获取ip和request，从而可以从上面的计算结果Map中得到这条记录对应的5分钟内总量，从而匹配数据库流程规则
      val ip = processedData.remoteAddr
      val request = processedData.request
      //反爬虫结果
      val antiCalculateResult = RuleUtil.calculateAntiResult(processedData, broadcastFlowList.value.toArray, ip, request,
        ipBlockAccessCountsMap, ipAccessCountsMap, criticalPagesCountsMap, userAgentCountsMap,
        criticalMinIntervalMap, acriticalPagesTimeMap, differentTripQuerysMap,
        criticalCookiesMap)
      antiCalculateResult
    }

    val antiBlackResults = antiCalculateResults.filter { antiCalculateResult =>
      val upLimitedFlows = antiCalculateResult.flowsScore.filter { flowScore =>
        //阈值判断结果，打分值大于阈值，为true
        flowScore.isUpLimited
      }
      //数据非空，说明存在大于阈值的流程打分
      upLimitedFlows.nonEmpty
    }

    antiBlackResults.map { antiBlackResult =>(antiBlackResult.ip, antiBlackResult.flowsScore)
    }.foreachRDD { rdd =>
      //过滤掉重复的数据，（ip，流程分数）
      val distincted: RDD[(String, Array[FlowScoreResult])] = rdd.reduceByKey((x, y) => x)
      //反爬虫黑名单数据（ip，流程分数）
      val antiBlackList: Array[(String, Array[FlowScoreResult])] = distincted.collect()
    }
    antiBlackResults.map { antiBlackResult =>
      //黑名单ip，黑名单打分
      (antiBlackResult.ip, antiBlackResult.flowsScore)
    }.foreachRDD { rdd =>
      //过滤掉重复的数据，（ip，流程分数）
      val distincted: RDD[(String, Array[FlowScoreResult])] = rdd.reduceByKey((x, y) => x)
      //反爬虫黑名单数据（ip，流程分数）
      val antiBlackList: Array[(String, Array[FlowScoreResult])] = distincted.collect()
      if (antiBlackList.nonEmpty) {
        //黑名单DataFrame-备份到HDFS
        val antiBlackListDFR = new ArrayBuffer[Row]
        try {
          //创建jedis连接
          val jedis = JedisConnectionUtil.getJedisCluster
          /*
            *恢复redis黑名单数据，用于防止程序停止而产生的redis数据丢失
           */
          BlackListToRedis.blackListDataToRedis(jedis, sc, sqlContext)
          antiBlackList.foreach { antiBlackRecord =>
            //拿到某个反爬虫数据的打分信息 Array[FlowScoreResult])
            antiBlackRecord._2.foreach { antiBlackRecordByFlow =>
              //Redis基于key:field - value的方式保存黑名单
              //redis黑名单库中的键 ip:flowId
              val blackListKey = PropertiesUtil.getStringByKey("cluster.key.anti_black_list", "jedisConfig.properties") + s"${antiBlackRecord._1}:${antiBlackRecordByFlow.flowId}"
              //redis黑名单库中的值：flowScore|strategyCode|hitRules|time
              val blackListValue = s"${antiBlackRecordByFlow.flowScore}|${antiBlackRecordByFlow.flowStrategyCode}|${antiBlackRecordByFlow.hitRules.mkString(",")}|${antiBlackRecordByFlow.hitTime}"
              //更新黑名单库，超时时间为3600秒
              jedis.setex(blackListKey, PropertiesUtil.getStringByKey("cluster.exptime.anti_black_list", "jedisConfig.properties").toInt, blackListValue)
              //添加黑名单DataFrame-备份到ArrayBuffer
              antiBlackListDFR.append(Row(((PropertiesUtil.getStringByKey("cluster.exptime.anti_black_list", "jedisConfig.properties").toLong) * 1000 + System.currentTimeMillis()).toString, blackListKey, blackListValue))
            }
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    }

    lines.foreachRDD(antiCalculateResult =>{
      val date = new SimpleDateFormat("yyyy/MM/dd/HH").format(System.currentTimeMillis())
      val yyyyMMddHH = date.replace("/","").toInt
      val path: String = PropertiesUtil.getStringByKey("blackListPath","HDFSPathConfig.properties")+"itheima/"+yyyyMMddHH
      try{
        sc.textFile(path+"/part-00000").union(antiCalculateResult).repartition(1).saveAsTextFile(path)
      }catch{
        case e: Exception =>
          antiCalculateResult.repartition(1).saveAsTextFile(path)
      }    })

        ssc
  }
}
