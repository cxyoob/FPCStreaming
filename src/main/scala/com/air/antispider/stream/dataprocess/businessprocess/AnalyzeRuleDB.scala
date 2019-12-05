package com.air.antispider.stream.dataprocess.businessprocess

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.air.antispider.stream.common.bean.{AnalyzeRule, FlowCollocation, RuleCollocation}
import com.air.antispider.stream.common.util.database.{QueryDB, c3p0Util}
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object AnalyzeRuleDB {

  def createRuleList(process_id: String, n: Int): List[RuleCollocation] = {
    var list = new ListBuffer[RuleCollocation]

    val sql = "select * from(select nh_rule.id,nh_rule.process_id,nh_rules_maintenance_table.rule_real_name,nh_rule.rule_type,nh_rule.crawler_type,"+ "nh_rule.status,nh_rule.arg0,nh_rule.arg1,nh_rule.score from nh_rule,nh_rules_maintenance_table where nh_rules_maintenance_table."+ "rule_name=nh_rule.rule_name) as tab where process_id = '"+process_id + "'and crawler_type="+n
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs:ResultSet = null
    try{
      conn = c3p0Util.getConnection
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      while ( rs.next() ) {
        val ruleId = rs.getString("id")
        val flowId = rs.getString("process_id")
        val ruleName = rs.getString("rule_real_name")
        val ruleType = rs.getString("rule_type")
        val ruleStatus = rs.getInt("status")
        val ruleCrawlerType = rs.getInt("crawler_type")
        val ruleValue0 = rs.getDouble("arg0")
        val ruleValue1 = rs.getDouble("arg1")
        val ruleScore = rs.getInt("score")
        val ruleCollocation = new RuleCollocation(ruleId,flowId,ruleName,ruleType,ruleStatus,ruleCrawlerType,ruleValue0,ruleValue1,ruleScore)
        list += ruleCollocation
      }

    }catch {
      case e : Exception => e.printStackTrace()
    }finally {
      c3p0Util.close(conn, ps, rs)
    }
    list.toList
  }

  def createFlow(n: Int) = {
    var array = new ArrayBuffer[FlowCollocation]
    var sql:String = ""
    if(n == 0){ sql = "select nh_process_info.id,nh_process_info.process_name,nh_strategy.crawler_blacklist_thresholds from nh_process_info,nh_strategy where nh_process_info.id=nh_strategy.id and status=0"}
    else if(n == 1){sql = "select nh_process_info.id,nh_process_info.process_name,nh_strategy.occ_blacklist_thresholds from nh_process_info,nh_strategy where nh_process_info.id=nh_strategy.id and status=1"}
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs:ResultSet = null
    try{
      conn = c3p0Util.getConnection
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      while (rs.next()) {
        val flowId = rs.getString("id")
        val flowName = rs.getString("process_name")
        if(n == 0){
          val flowLimitScore = rs.getDouble("crawler_blacklist_thresholds")
          array += new FlowCollocation(flowId, flowName,createRuleList(flowId,n), flowLimitScore, flowId)
        }else if(n == 1){
          val flowLimitScore = rs.getDouble("occ_blacklist_thresholds")
          array += new FlowCollocation(flowId, flowName,createRuleList(flowId,n), flowLimitScore, flowId)
        }

      }

    }catch{
      case e : Exception => e.printStackTrace()
    }finally {
      c3p0Util.close(conn, ps, rs)
    }

    array
  }

  def queryIpListToBrocast() = {
    //mysql中ip黑名单数据
    val nibsql = "select ip_name from nh_ip_blacklist"
    val nibField = "ip_name"
    val ipInitList = QueryDB.queryData(nibsql, nibField)

    ipInitList
  }

  def queryCriticalPages() = {
    val queryCriticalPagesSql = "select criticalPageMatchExpression from nh_query_critical_pages"
    val queryCriticalPagesField = "criticalPageMatchExpression"
    val queryCriticalPages = QueryDB.queryData(queryCriticalPagesSql, queryCriticalPagesField)

    queryCriticalPages
  }

  def queryFilterRule()={
    val sql = "select value from nh_filter_rule"
    val filed = "value"
    val arr: ArrayBuffer[String] = QueryDB.queryData(sql,filed)
    arr
  }
  def queryQueryRules() = {
    val sql = "select * from analyzerule"
//    val all = "id,flight_type,behavior_type,requestMatchExpression,requestMethod,isNormalGet," +
//      "isNormalForm,isApplicationJson,isTextXml,isJson,isXML,formDataField" +
//      "book_bookUserId,book_bookUnUserId,book_psgName,book_psgType,book_idType,book_idCard" +
//      "book_contractName,book_contractPhone,book_depCity,book_arrCity,book_flightDate" +
//      "book_flightNo,book_cabin,query_depCity,query_arrCity,query_flightDate,query_adultNum" +
//      "query_childNum,query_infantNum,query_country,query_travelType,book_psgFirName"
    val id = "id"
    val flightType = "flight_type"
    val BehaviorType = "behavior_type"
    val requestMatchExpression = "requestMatchExpression"
    val requestMethod = "requestMethod"
    val isNormalGet = "isNormalGet"
    val isNormalForm = "isNormalForm"
    val isApplicationJson = "isApplicationJson"
    val isTextXml = "isTextXml"
    val isJson = "isJson"
    val isXML = "isXML"
    val formDataField = "formDataField"
    val book_bookUserId = "book_bookUserId"
    val book_bookUnUserId = "book_bookUnUserId"
    val book_psgName = "book_psgName"
    val book_psgType = "book_psgType"
    val book_idType = "book_idType"
    val book_idCard = "book_idCard"
    val book_contractName = "book_contractName"
    val book_contractPhone = "book_contractPhone"
    val book_depCity = "book_depCity"
    val book_arrCity = "book_arrCity"
    val book_flightDate = "book_flightDate"
    val book_flightNo = "book_flightNo"
    val book_cabin = "book_cabin"
    val query_depCity = "query_depCity"
    val query_arrCity = "query_arrCity"
    val query_flightDate = "query_flightDate"
    val query_adultNum = "query_adultNum"
    val query_childNum = "query_childNum"
    val query_infantNum = "query_infantNum"
    val query_country = "query_country"
    val query_travelType = "query_travelType"
    val book_psgFirName = "book_psgFirName"
    val ids: ArrayBuffer[String] = QueryDB.queryData(sql, id)
    val flightTypes: ArrayBuffer[String] = QueryDB.queryData(sql, flightType)
    val BehaviorTypes: ArrayBuffer[String] = QueryDB.queryData(sql, BehaviorType)
    val requestMatchExpressions: ArrayBuffer[String] = QueryDB.queryData(sql, requestMatchExpression)
    val requestMethods: ArrayBuffer[String] = QueryDB.queryData(sql, requestMethod)
    val isNormalGets: ArrayBuffer[String] = QueryDB.queryData(sql, isNormalGet)
    val isNormalForms: ArrayBuffer[String] = QueryDB.queryData(sql, isNormalForm)
    val isApplicationJsons: ArrayBuffer[String] = QueryDB.queryData(sql, isApplicationJson)
    val isTextXmls: ArrayBuffer[String] = QueryDB.queryData(sql, isTextXml)
    val isJsons: ArrayBuffer[String] = QueryDB.queryData(sql, isJson)
    val isXMLs: ArrayBuffer[String] = QueryDB.queryData(sql, isXML)
    val formDataFields: ArrayBuffer[String] = QueryDB.queryData(sql, formDataField)
    val book_bookUserIds: ArrayBuffer[String] = QueryDB.queryData(sql, book_bookUserId)
    val book_bookUnUserIds: ArrayBuffer[String] = QueryDB.queryData(sql, book_bookUnUserId)
    val book_psgNames: ArrayBuffer[String] = QueryDB.queryData(sql, book_psgName)
    val book_psgTypes: ArrayBuffer[String] = QueryDB.queryData(sql, book_psgType)
    val book_idTypes: ArrayBuffer[String] = QueryDB.queryData(sql, book_idType)
    val book_idCards: ArrayBuffer[String] = QueryDB.queryData(sql, book_idCard)
    val book_contractNames: ArrayBuffer[String] = QueryDB.queryData(sql, book_contractName)
    val book_contractPhones: ArrayBuffer[String] = QueryDB.queryData(sql, book_contractPhone)
    val book_depCitys: ArrayBuffer[String] = QueryDB.queryData(sql, book_depCity)
    val book_arrCitys: ArrayBuffer[String] = QueryDB.queryData(sql, book_arrCity)
    val book_flightDates: ArrayBuffer[String] = QueryDB.queryData(sql, book_flightDate)
    val book_flightNos: ArrayBuffer[String] = QueryDB.queryData(sql, book_flightNo)
    val book_cabins: ArrayBuffer[String] = QueryDB.queryData(sql, book_cabin)
    val query_depCitys: ArrayBuffer[String] = QueryDB.queryData(sql, query_depCity)
    val query_arrCitys: ArrayBuffer[String] = QueryDB.queryData(sql, query_arrCity)
    val query_flightDates: ArrayBuffer[String] = QueryDB.queryData(sql, query_flightDate)
    val query_adultNums: ArrayBuffer[String] = QueryDB.queryData(sql, query_adultNum)
    val query_childNums: ArrayBuffer[String] = QueryDB.queryData(sql, query_childNum)
    val query_infantNums: ArrayBuffer[String] = QueryDB.queryData(sql, query_infantNum)
    val query_countrys: ArrayBuffer[String] = QueryDB.queryData(sql, query_country)
    val query_travelTypes: ArrayBuffer[String] = QueryDB.queryData(sql, query_travelType)
    val book_psgFirNames: ArrayBuffer[String] = QueryDB.queryData(sql, book_psgFirName)
    val buffer: ArrayBuffer[AnalyzeRule] = new ArrayBuffer
    for (i <- 0 until ids.length) {
      val analyzeRule: AnalyzeRule = new AnalyzeRule
      analyzeRule.id = ids(i)
      analyzeRule.flightType = flightTypes(i).toInt
      analyzeRule.BehaviorType = BehaviorTypes(i).toInt
      analyzeRule.requestMatchExpression = requestMatchExpressions(i)
      analyzeRule.requestMethod = requestMethods(i)
      analyzeRule.isNormalGet = isNormalGets(i).toBoolean
      analyzeRule.isNormalForm = isNormalForms(i).toBoolean
      analyzeRule.isApplicationJson = isApplicationJsons(i).toBoolean
      analyzeRule.isTextXml = isTextXmls(i).toBoolean
      analyzeRule.isJson = isJsons(i).toBoolean
      analyzeRule.isXML = isXMLs(i).toBoolean
      analyzeRule.formDataField = formDataFields(i)
      analyzeRule.book_bookUserId = book_bookUserIds(i)
      analyzeRule.book_bookUnUserId = book_bookUnUserIds(i)
      analyzeRule.book_psgName = book_psgNames(i)
      analyzeRule.book_psgType = book_psgTypes(i)
      analyzeRule.book_idType = book_idTypes(i)
      analyzeRule.book_idCard = book_idCards(i)
      analyzeRule.book_contractName = book_contractNames(i)
      analyzeRule.book_contractPhone = book_contractPhones(i)
      analyzeRule.book_depCity = book_depCitys(i)
      analyzeRule.book_arrCity = book_arrCitys(i)
      analyzeRule.book_flightDate = book_flightDates(i)
      analyzeRule.book_flightNo = book_flightNos(i)
      analyzeRule.book_cabin = book_cabins(i)
      analyzeRule.query_depCity = query_depCitys(i)
      analyzeRule.query_arrCity = query_arrCitys(i)
      analyzeRule.query_flightDate = query_flightDates(i)
      analyzeRule.query_adultNum = query_adultNums(i)
      analyzeRule.query_childNum = query_childNums(i)
      analyzeRule.query_infantNum = query_infantNums(i)
      analyzeRule.query_country = query_countrys(i)
      analyzeRule.query_travelType = query_travelTypes(i)
      analyzeRule.book_psgFirName = book_psgFirNames(i)
      buffer.+=(analyzeRule)
    }
    buffer.toList
  }


  def queryRuleMap(): java.util.Map[String, ArrayBuffer[String]] = {

    //从数据库中查找航班分类规则-国内查询
    val nqsql = "select expression from nh_classify_rule where flight_type = " + FlightTypeEnum.National.id + " and operation_type = " + BehaviorTypeEnum.Query.id
    //从数据库中查找航班分类规则-国际查询
    val iqsql = "select expression from nh_classify_rule where flight_type = " + FlightTypeEnum.International.id + " and operation_type = " + BehaviorTypeEnum.Query.id
    //从数据库中查找航班分类规则-国内预定
    val nbsql = "select expression from nh_classify_rule where flight_type = " + FlightTypeEnum.National.id + " and operation_type = " + BehaviorTypeEnum.Book.id

    //从数据库中查找航班分类规则-国际预定

    val ibsql = "select expression from nh_classify_rule where flight_type = " + FlightTypeEnum.International.id + " and operation_type = " + BehaviorTypeEnum.Book.id

    val ncrField = "expression"

    val ruleMapTemp: java.util.Map[String, ArrayBuffer[String]] = new java.util.HashMap[String, ArrayBuffer[String]]

    val nationalQueryList = QueryDB.queryData(nqsql, ncrField)

    val internationalQueryList = QueryDB.queryData(iqsql, ncrField)

    val nationalBookList = QueryDB.queryData(nbsql, ncrField)

    val internationalBookList = QueryDB.queryData(ibsql, ncrField)

    ruleMapTemp.put("nationalQuery", nationalQueryList)

    ruleMapTemp.put("internationalQuery", internationalQueryList)

    ruleMapTemp.put("nationalBook", nationalBookList)

    ruleMapTemp.put("internationalBook", internationalBookList)

    ruleMapTemp

  }

  def queryIpBlackList() ={
    val sql = "select ip_name from nh_ip_blacklist"
    val field = "ip_name"
    val ipList = QueryDB.queryData(sql,field)
    ipList

  }


}
