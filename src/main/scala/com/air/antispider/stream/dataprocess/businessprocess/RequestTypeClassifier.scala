package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.RequestType
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum}

import scala.collection.mutable.ArrayBuffer

object RequestTypeClassifier {

  def classifyByRequest(request: String, map: java.util.Map[String, ArrayBuffer[String]]): RequestType = {

    //国内查询URL-正则表达式

    val nationalQueryList = map.get("nationalQuery")

    //国际查询URL-正则表达式

    val internationalQueryList = map.get("internationalQuery")

    //国内预定URL-正则表达式

    val nationalBookList = map.get("nationalBook")

    //国际预定URL-正则表达式

    val internationalBookList = map.get("internationalBook")

    //标记，如果哪个都没匹配上，另外打标记

    var flag = true

    //请求类型

    var requestType: RequestType = null

    //国内查询循环匹配

    for (nqTemp <- 0 until nationalQueryList.size if flag) {

      //匹配上

      if (request.matches(nationalQueryList(nqTemp))) {

        //匹配上，设置为false，不用另外打标记

        flag = false

        //将请求类型封装到RequestType中返回

        requestType = RequestType(FlightTypeEnum.National, BehaviorTypeEnum.Query)

      }

    }



    for (iqTemp <- 0 until internationalQueryList.size if flag) {

      if (request.matches(internationalQueryList(iqTemp))) {

        flag = false

        requestType = RequestType(FlightTypeEnum.International, BehaviorTypeEnum.Query)

      }

    }



    for (nbTemp <- 0 until nationalBookList.size if flag) {

      if (request.matches(nationalBookList(nbTemp))) {

        flag = false

        requestType = RequestType(FlightTypeEnum.National, BehaviorTypeEnum.Book)

      }

    }



    for (ibTemp <- 0 until internationalBookList.size if flag) {

      if (request.matches(internationalBookList(ibTemp))) {

        flag = false

        requestType = RequestType(FlightTypeEnum.International, BehaviorTypeEnum.Book)

      }

    }

    //没匹配上，打个other的标记

    if (flag) {

      requestType = RequestType(FlightTypeEnum.Other, BehaviorTypeEnum.Other)

    }

    //返回标记

    requestType

  }


}
