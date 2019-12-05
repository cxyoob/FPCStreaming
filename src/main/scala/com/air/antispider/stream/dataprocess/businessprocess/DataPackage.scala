package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.{BookRequestData, CoreRequestParams, ProcessedData, QueryRequestData, RequestType}
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum.TravelTypeEnum

object DataPackage {

  def dataPackage(sourceData: String, requestMethod: String, request: String,

                  remoteAddr: String, httpUserAgent: String, timeIso8601: String,

                  serverAddr: String,  highFrqIPGroup: Boolean,

                  requestTypeLabel: RequestType, travelType: TravelTypeEnum,

                  cookieValue_JSESSIONID: String, cookieValue_USERID: String,

                  queryRequestData: Option[QueryRequestData], bookRequestData: Option[BookRequestData],

                  httpReferrer: String):ProcessedData={

    var flightDate = ""

    bookRequestData match {

      case Some(book) => flightDate = book.flightDate.mkString

      case None =>

    }

    queryRequestData match {

      case Some(value) => flightDate = value.flightDate

      case None =>

    }



    var depCity = ""

    bookRequestData match {

      case Some(book) => depCity = book.depCity.mkString

      case None =>

    }

    queryRequestData match {

      case Some(value) => depCity = value.depCity

      case None =>

    }



    var arrCity = ""

    bookRequestData match {

      case Some(book) => arrCity = book.arrCity.mkString

      case None =>

    }

    queryRequestData match {

      case Some(value) => arrCity = value.arrCity

      case None =>

    }

    //主要请求参数

    val requestParams = CoreRequestParams(flightDate, depCity, arrCity)

    ProcessedData("", requestMethod, request, remoteAddr, httpUserAgent,

      timeIso8601, serverAddr,  highFrqIPGroup,

      requestTypeLabel, travelType, requestParams, cookieValue_JSESSIONID,

      cookieValue_USERID, queryRequestData, bookRequestData, httpReferrer)

  }


}
