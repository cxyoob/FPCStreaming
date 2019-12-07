package com.csair.b2c.gciantispider.offlinecalculation.statisticaloffline

import java.sql.Date
import java.util.UUID

import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import com.air.antispider.stream.common.util.log4j.LoggerLevels
import com.csair.b2c.gciantispider.util.SparkMySqlProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}


case class Requests(requestMethod: String, request: String, remoteAddr: String,
                    httpUserAgent: String, timeIso8601: String, serverAddr: String,
                    criticalCookie: String, highFrqIPGroup: String, flightType: String,
                    behaviorType: String, travelType: String, flightDate: String,
                    depcity: String, arrcity: String, JSESSIONID: String,
                    USERID: String, queryRequestDataStr: String, bookRequestDataStr: String,
                    httpReferrer: String, StageTag: String, Spider: Int)


case class BlackList(remoteAddr: String,
                     FlowID: String,
                     Score: String,
                     StrategicID: String)



object VisualizationIndicators {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local").setAppName("visualization").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //数据路径
    val defaultPathConfig = "offlineConfig.properties"
    val filePath = PropertiesUtil.getStringByKey("inputFilePath", defaultPathConfig)
    //读取kafka中的原始日志信息
    val request = sc.textFile("")
      .map(
        _.split("#CS#")).map(
      p => Requests(p(1), p(2), p(3), p(4), p(5), p(6), "", p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), "", 0)).toDF()
    //注册为表request
    request.registerTempTable("request")
    //读取黑名单信息
    val SpiderIP = sc.textFile("").map(_.split("\\|")).map(p => BlackList(p(0), p(1), p(2), p(3))).toDF()
    //注册为表SpiderIP
    SpiderIP.registerTempTable("SpiderIP")
    val AddSpiderTag = request.join(SpiderIP, request("remoteAddr") === SpiderIP("remoteAddr"), "left_outer")
    //查询转化率必须的字段
    val TransformRateNeeded = AddSpiderTag.select(AddSpiderTag("request"), AddSpiderTag("JSESSIONID"), AddSpiderTag("StageTag"), AddSpiderTag("FlowID"), AddSpiderTag("travelType"), AddSpiderTag("flightType"))
    //注册为TransformRateNeeded表
    TransformRateNeeded.registerTempTable("TransformRateNeeded")
    val date: String = AddSpiderTag.select(AddSpiderTag("timeIso8601")).first().get(0).toString.split("T")(0)
    val dataTime: Date = Date.valueOf(date)
    sqlContext.udf.register("Stage", (request: String) => {
      if (request.matches("^./bookingnew/.$") //预定、商城
        || request.matches("^./bookingGroup/.$")
        || request.matches("^./ita/intl/zh/shop/.$")) {
        "1"
      } else if (request.matches("^./modules/permissionnew/.$") //权限、乘客信息
        || request.matches("^./ita/intl/zh/passengers/.$")) {
        "2"
      } else if (request.matches("^.upp_payment/pay/.$")) {
        //支付
        "3"
      } else {
        //其他，查询等操作
        "0"
      }
    })
    //FlowID不为空时对应为爬虫
    sqlContext.udf.register("Spider", (FlowID: String) => {
      if (FlowID == null || "null".equalsIgnoreCase(FlowID)) {
        //非爬虫
        "0"
      } else {
        //爬虫
        "1"
      }
    })
    //原始数据：request,JSESSIONID,Stage(request) as StageTag, Spider(FlowID) as SpiderTag, flightType, travelType
    val request1 = sqlContext.sql("select request,JSESSIONID,Stage(request) as StageTag, Spider(FlowID) as SpiderTag, flightType, travelType from TransformRateNeeded")
    //从表TransformRateNeeded中过滤掉JSESSIONID为空的数据
    val request1_transformed = request1.filter(!request1("JSESSIONID").contains("NULL"))
    val NatinalRate_1 = request1_transformed.filter(request1_transformed("flightType").equalTo("National")).filter(request1_transformed("StageTag").equalTo("2")).count().toFloat /
    request1.filter(request1("flightType").equalTo("National")).filter(request1("StageTag").equalTo("1")).count().toFloat
    //通过并行化创建RDD
    val nh_domestic_inter_conversion_rate_RDD = sc.parallelize(Array(UUID.randomUUID().toString() + "," + "0," + "0," + NatinalRate_1)).map(_.split(","))
    //通过StructType直接指定每个字段的schema
    val schema = StructType(List(StructField("id", StringType, true), StructField("step_type", IntegerType, true), StructField("flight_type", IntegerType, true), StructField("conversion_rate", FloatType, true), StructField("record_time", DateType, true)))
    //将RDD映射到rowRDD
    val rowRDD = nh_domestic_inter_conversion_rate_RDD.map(p => Row(p(0), p(1).toInt, p(2).toInt, p(3).toFloat, dataTime))
    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    //将数据追加到数据库
    personDataFrame.write.mode("append").jdbc("jdbc:mysql://192.168.56.204:3306/gciantispider", "gciantispider.nh_domestic_inter_conversion_rate", SparkMySqlProperties.getProperty())
    println("国内查询转化率")
    println(NatinalRate_1)
    val InternatinalRate_1 = request1_transformed.filter(request1_transformed("flightType").equalTo("Internatinal")).filter(request1_transformed("StageTag").equalTo("2")).count().toFloat /
    request1.filter(request1("flightType").equalTo("Internatinal")).filter(request1("StageTag").equalTo("1")).count().toFloat
    //通过并行化创建RDD
    val nh_domestic_inter_conversion_rate_RDD2 = sc.parallelize(Array(UUID.randomUUID().toString() + "," + "0," + "1," + InternatinalRate_1)).map(_.split(","))
    //通过StructType直接指定每个字段的schema
    val rowRDD2 = nh_domestic_inter_conversion_rate_RDD2.map(p => Row(p(0), p(1).toInt, p(2).toInt, p(3).toFloat, dataTime))
    //将schema信息应用到rowRDD上
    val personDataFrame2 = sqlContext.createDataFrame(rowRDD2, schema)
    //将数据追加到数据库
    personDataFrame2.write.mode("append").jdbc("jdbc:mysql://192.168.56.204:3306/gciantispider", "gciantispider.nh_domestic_inter_conversion_rate", SparkMySqlProperties.getProperty())
    println("国际查询转化率")
    println(InternatinalRate_1)
    val NatinalRate_2 = request1_transformed.filter(request1_transformed("flightType").equalTo("National")).filter(request1_transformed("StageTag").equalTo("3")).count().toFloat /
    request1.filter(request1("flightType").equalTo("National")).filter(request1("StageTag").equalTo("2")).count().toFloat
    println("国内航班选择-旅客信息 转化率")
    println(NatinalRate_2)
    val InternatinalRate_2 = request1_transformed.filter(request1_transformed("flightType").equalTo("Internatinal")).filter(request1_transformed("StageTag").equalTo("3")).count().toFloat /
    request1.filter(request1("flightType").equalTo("Internatinal")).filter(request1("StageTag").equalTo("2")).count().toFloat
    println("国际航班选择-旅客信息 转化率")
    println(InternatinalRate_2)


  }
}