package com.csair.b2c.gciantispider.util

import java.util.Properties

object SparkMySqlProperties {
  def getProperty(): Properties = {
    val properties: Properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","wenbo123456")
    properties
  }

}
