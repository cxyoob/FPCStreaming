package com.air.antispider.stream.rulecompute.businessprocess

import com.air.antispider.stream.common.bean.FlowCollocation
import com.air.antispider.stream.dataprocess.businessprocess.AnalyzeRuleDB
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

object BroadcastProcess {

  def monitorFlowListRule(
                           sc: SparkContext,
                           broadcastFlowList: Broadcast[ArrayBuffer[FlowCollocation]],
                           jedis: JedisCluster): Broadcast[ArrayBuffer[FlowCollocation]] = {
    // 查询redis中的标识
    val needUpdateFlowList = jedis.get("QueryFlowChangeFlag")
    if(needUpdateFlowList!=null
      && !needUpdateFlowList.isEmpty &&
      needUpdateFlowList.toBoolean){
      // 查询数据库规则
      val FlowList = AnalyzeRuleDB.createFlow(0)
      // 删除广播变量
      broadcastFlowList.unpersist()
      // 重新设置广播标识
      jedis.set("QueryFlowChangeFlag","false")
      // 重新广播
      sc.broadcast(FlowList)
    }else{
      // 如果没有更新就返回当前广播变量
      broadcastFlowList
    }
  }

  def monitorIpBlackListRule(sc: SparkContext,
                             broadcastIPList: Broadcast[ArrayBuffer[String]],
                             jedis: JedisCluster): Broadcast[ArrayBuffer[String]] = {
    // 查询redis中的标识
    val needUpdateIPList = jedis.get("IpBlackRuleChangeFlag")
    if(needUpdateIPList!=null
      && !needUpdateIPList.isEmpty &&
      needUpdateIPList.toBoolean){
      // 查询数据库规则
      val IPList = AnalyzeRuleDB.queryIpBlackList()
      // 删除广播变量
      broadcastIPList.unpersist()
      // 重新设置广播标识
      jedis.set("IpBlackRuleChangeFlag","false")
      // 重新广播
      sc.broadcast(IPList)
    }else{
      // 如果没有更新就返回当前广播变量
      broadcastIPList
    }
  }

  def monitorQueryCriticalPagesRule(sc: SparkContext,
                                    broadcastQueryCriticalPages: Broadcast[ArrayBuffer[String]],
                                    jedis: JedisCluster): Broadcast[ArrayBuffer[String]] = {
    // 查询redis中的标识
    val needUpdateQueryCriticalPages = jedis.get("QueryCriticalPages")
    if(needUpdateQueryCriticalPages!=null
      && !needUpdateQueryCriticalPages.isEmpty &&
      needUpdateQueryCriticalPages.toBoolean){
      // 查询数据库规则
      val QueryCriticalPages = AnalyzeRuleDB.queryCriticalPages()
      // 删除广播变量
      broadcastQueryCriticalPages.unpersist()
      // 重新设置广播标识
      jedis.set("QueryCriticalPages","false")
      // 重新广播
      sc.broadcast(QueryCriticalPages)
    }else{
      // 如果没有更新就返回当前广播变量
      broadcastQueryCriticalPages
    }
  }

}
