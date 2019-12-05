package com.air.antispider.stream.dataprocess.businessprocess

import java.util.logging.Logger

import com.air.antispider.stream.common.bean.RequestType
import com.air.antispider.stream.common.util.xml.xmlUtil
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, TravelTypeEnum}
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum.TravelTypeEnum

object TravelTypeClassifier {

  val logger = Logger.getLogger("AnalyzeRequest")

  def classifyByRefererAndRequestBody(requestTypeLabel: RequestType, httpReferrer: String, requestBody: String): TravelTypeEnum = {

    //创建往返类型标记类

    var result = TravelTypeEnum(-1)

    //操作类型为查询

    if (BehaviorTypeEnum.Query == requestTypeLabel.behaviorType) {

      var dateCounts = 0

      //日期正则

      val regex = "^(\\d{4})-(0\\d{1}|1[0-2])-(0\\d{1}|[12]\\d{1}|3[01])$"

      //用？分割

      if (httpReferrer.contains("?") && httpReferrer.split("\\?").length > 1) {

        //先用？分割，再用&分割

        val params = httpReferrer.split("\\?")(1).split("&")

        //取到所有的参数循环

        for (param <- params) {

          //用“=”分割

          val keyAndValue = param.split("=")

          //正则匹配日期

          if (keyAndValue.length > 1 && keyAndValue(1).matches(regex)) {

            dateCounts = dateCounts + 1

          }

        }

      }
      if (dateCounts == 1) {

        //一个日期为单程

        result = TravelTypeEnum.OneWay

      } else if (dateCounts == 2) {

        //两个日期为往返

        result = TravelTypeEnum.RoundTrip

      } else {

        //否则啥都不是

        result = TravelTypeEnum.Unknown

      }

    }

    //操作类型为预定：无法拿到订单数据，暂时不做

    if (BehaviorTypeEnum.Book == requestTypeLabel.behaviorType) {

      result = xmlUtil.getTravelType(requestBody)

    }

    result

  }



  def classifyByReferer(httpReferrer: String): TravelTypeEnum = {

    var dateCounts = 0

    val regex = "^(\\d{4})-(0\\d{1}|1[0-2])-(0\\d{1}|[12]\\d{1}|3[01])$"

    if (httpReferrer.contains("?") && httpReferrer.split("\\?").length > 1) {

      val params = httpReferrer.split("\\?")(1).split("&")

      for (param <- params) {

        val keyAndValue = param.split("=")

        if (keyAndValue.length > 1 && keyAndValue(1).matches(regex)) {

          dateCounts = dateCounts + 1

        }

      }

    }

    if (dateCounts == 1) {

      TravelTypeEnum.OneWay

    } else if (dateCounts == 2) {

      TravelTypeEnum.RoundTrip

    } else {

      TravelTypeEnum.Unknown

    }

  }

}
