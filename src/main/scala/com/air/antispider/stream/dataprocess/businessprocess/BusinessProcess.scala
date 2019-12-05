package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.AccessLog
import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import redis.clients.jedis.JedisCluster

object BusinessProcess {
  def linkCount(accessLog: RDD[AccessLog], jedis: JedisCluster)= {
    // 统计链路 数据量
    val serverCount = accessLog.map(rdd=>(rdd.server_addr,1))
      .reduceByKey(_+_)
    // 活跃连接数
    val activeNum = accessLog.map(rdd=>{
      (rdd.server_addr,rdd.connectionActive)
    }).reduceByKey((x,y)=>y)
    //    activeNum.foreach(t=> println(t._2))
    // 将数据存入Redis
    if(!serverCount.isEmpty() && !activeNum.isEmpty()){
      // 将两个RDD转换成数据
      val serverCountMap= serverCount.collectAsMap()
      val activeNumMap = activeNum.collectAsMap()
      // 将结果进行处理
      val map = Map(
        // 名字不能随便写，这里需要对接前端展示
        "activeNumMap"->activeNumMap,
        "serversCountMap"->serverCountMap
      )
      try {
        // 获取链路统计的Key
        val keyName = PropertiesUtil.getStringByKey(
          "cluster.key.monitor.linkProcess","jedisConfig.properties")+System.currentTimeMillis()
        // 设置链路统计的超时时间
        val expTime = PropertiesUtil.getStringByKey(
          "cluster.exptime.monitor","jedisConfig.properties")
        // 将数据写入
        jedis.setex(keyName,expTime.toInt,Json(DefaultFormats).write(map))
      }catch {
        case e:Exception=>{
          e.printStackTrace()
          // 关闭连接
          jedis.close()
        }
      }
    }
    serverCount
  }

}
