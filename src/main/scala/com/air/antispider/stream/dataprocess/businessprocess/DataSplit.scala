package com.air.antispider.stream.dataprocess.businessprocess

import java.util.regex.Pattern

import com.air.antispider.stream.common.bean.AccessLog
import com.air.antispider.stream.common.util.decode.{EscapeToolBox, RequestDecoder}
import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import org.apache.spark.rdd.RDD

object DataSplit {
  def parseAccessLog(msgRdd: RDD[String]): RDD[AccessLog] = {
    msgRdd.map(str=>{
      val values = str.split("#CS#",-1)
      val Array(time_local, request, request_method,
      content_type, request_body, http_referer,
      remote_addr, http_user_agent, time_iso8601,
      server_addr, http_cookie
      )=values
      var cookie = EntcryedData.encryptedPhone(http_cookie)
      cookie = EntcryedData.encryptedId(http_cookie)

      //提取Cookie信息并保存为K-V形式
      val cookieMap = {
        var tempMap = new scala.collection.mutable.HashMap[String, String]
        if (!cookie.equals("")) {
          cookie.split(";").foreach { s =>
            val kv = s.split("=")
            //UTF8解码
            if (kv.length > 1) {
              try {
                val chPattern = Pattern.compile("u([0-9a-fA-F]{4})")
                val chMatcher = chPattern.matcher(kv(1))
                var isUnicode = false
                while (chMatcher.find()) {
                  isUnicode = true
                }
                if (isUnicode) {
                  tempMap += (kv(0) -> EscapeToolBox.unescape(kv(1)))
                } else {
                  tempMap += (kv(0) -> RequestDecoder.decodePostRequest(kv(1)))
                }
              } catch {
                case e: Exception => e.printStackTrace()
              }
            }
          }
        }
        tempMap
      }
      //Cookie关键信息解析
      //从配置文件读取Cookie配置信息
      val cookieKey_JSESSIONID = PropertiesUtil.getStringByKey(
        "cookie.JSESSIONID.key", "cookieConfig.properties")
      val cookieKey_userId4logCookie = PropertiesUtil.getStringByKey(
        "cookie.userId.key", "cookieConfig.properties")
      //Cookie-JSESSIONID
      val cookieValue_JSESSIONID = cookieMap.getOrElse(cookieKey_JSESSIONID, "NULL")
      //Cookie-USERID-用户ID
      val cookieValue_USERID = cookieMap.getOrElse(" "+cookieKey_userId4logCookie, "NULL")

      AccessLog(time_local, request, request_method,
        content_type, request_body, http_referer,
        remote_addr, http_user_agent, time_iso8601,
        server_addr, cookie, 0,cookieValue_JSESSIONID,cookieValue_USERID)
    })
  }


}
