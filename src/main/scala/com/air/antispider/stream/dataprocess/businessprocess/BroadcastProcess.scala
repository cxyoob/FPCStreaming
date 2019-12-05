package com.air.antispider.stream.dataprocess.businessprocess

import java.util

import com.air.antispider.stream.common.bean.AnalyzeRule
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

object BroadcastProcess {


  def BroadFilterRule(sc: SparkContext,
                      filterRuleRef: Broadcast[ArrayBuffer[String]],
                      jedis: JedisCluster
                     )={
    val needUpdateFiletList: String = jedis.get("FilterChangeFlag")
    if(needUpdateFiletList!=null &&
      !needUpdateFiletList.isEmpty &&
      needUpdateFiletList.toBoolean){
      val filterRuleListUpdate: ArrayBuffer[String] = AnalyzeRuleDB.queryFilterRule()
      filterRuleRef.unpersist()
      jedis.set("FilterChangeFlag","false")
      sc.broadcast(filterRuleListUpdate)
    }else{
      filterRuleRef
    }
  }

  def BroadRuleMap(sc: SparkContext,
                   broadcastRuleMap: Broadcast[util.Map[String, ArrayBuffer[String]]],
                      jedis: JedisCluster
                     )={
    val needUpdateClassifyRule = jedis.get("ClassifyRuleChangeFlag")
    //Mysql-规则是否改变标识
    if (needUpdateClassifyRule!=null &&
      !needUpdateClassifyRule.isEmpty() &&
      needUpdateClassifyRule.toBoolean) {
      val ruleMapTemp = AnalyzeRuleDB.queryRuleMap()
      broadcastRuleMap.unpersist()
      jedis.set("ClassifyRuleChangeFlag", "false")
      sc.broadcast(ruleMapTemp)
    }else{
      broadcastRuleMap
    }
  }

  def BroadQueryRule(sc: SparkContext,
                     broadcastQueryRulesList: Broadcast[List[AnalyzeRule]],
                      jedis: JedisCluster
                     )={
    val needUpdateQueryRulesList: String = jedis.get("AnalyzeruleChangeFlag")
    if(needUpdateQueryRulesList!=null &&
      !needUpdateQueryRulesList.isEmpty &&
      needUpdateQueryRulesList.toBoolean){
      val queryRulesListUpdate: List[AnalyzeRule] = AnalyzeRuleDB.queryQueryRules()
      broadcastQueryRulesList.unpersist()
      jedis.set("AnalyzeruleChangeFlag","false")
      sc.broadcast(queryRulesListUpdate)
    }else{
      broadcastQueryRulesList
    }
  }

  def mointorBlackListRule(
                            sc: SparkContext,
                            ipBlackListRef: Broadcast[ArrayBuffer[String]],
                            jedis: JedisCluster): Broadcast[ArrayBuffer[String]] = {
    //请求规则变更标识
    val needUpdateIpBlackRule = jedis.get("IpBlackRuleChangeFlag")
    //Mysql-规则是否改变标识
    if (needUpdateIpBlackRule!=null && !needUpdateIpBlackRule.isEmpty() && needUpdateIpBlackRule.toBoolean) {
      // 查询规则
      val ipBlackList = AnalyzeRuleDB.queryIpBlackList()
      // 释放广播变量
      ipBlackListRef.unpersist()
      // 设置标识
      jedis.set("IpBlackRuleChangeFlag", "false")
      // 重新广播
      sc.broadcast(ipBlackList)
    }else{
      // 如果没有更新 返回当前广播变量
      ipBlackListRef
    }
  }


}
