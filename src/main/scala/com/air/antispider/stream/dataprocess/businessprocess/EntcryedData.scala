package com.air.antispider.stream.dataprocess.businessprocess

import java.util.regex.{Matcher, Pattern}

import com.air.antispider.stream.common.util.decode.MD5



object EntcryedData {
  def encryptedPhone(http_cookie: String):String ={
    val md5 = new MD5
    var cookie: String = http_cookie
    val phonePattern: Pattern = Pattern.compile("((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(17[0-9])|(18[0,5-9]))\\d{8}")
    val phoneMatcher: Matcher = phonePattern.matcher(http_cookie)
    while(phoneMatcher.find()){
      val lowIndex = http_cookie.indexOf(phoneMatcher.group()) - 1
      val highIndex = lowIndex + phoneMatcher.group().length() + 1
      val lowLetter = http_cookie.charAt(lowIndex).toString
      if (!lowLetter.matches("^[0-9]$")) {
        if (highIndex < http_cookie.length()) {
          val highLetter = http_cookie.charAt(highIndex).toString
          if (!highLetter.matches("^[0-9]$")) {
            cookie = cookie.replace(phoneMatcher.group(), md5.getMD5ofStr(phoneMatcher.group()))
          }
        }else{
          cookie = cookie.replace(phoneMatcher.group(), md5.getMD5ofStr(phoneMatcher.group()))
        }
      }
    }
    cookie
  }
  def encryptedId(http_cookie: String): String ={
    val md5 = new MD5
    // 使用局部变量
    var cookie = http_cookie
    // 身份证号正则 匹配两种身份证长度 18 或 15
    val idPattern = Pattern.compile("(\\d{18})|(\\d{17}(\\d|X|x))|(\\d{15})")
    // 匹配身份证号
    val idMatcher = idPattern.matcher(http_cookie)
    //    // 循环处理
    while (idMatcher.find()){
      val lowIndex = http_cookie.indexOf(idMatcher.group())-1
      // 身份证号的后一个index
      val highIndex = lowIndex+idMatcher.group().length + 1
      // 身份证号的前一个字符
      val lowLetter = http_cookie.charAt(lowIndex).toString
      // 匹配当前第一位不是数字
      if(!lowLetter.matches("^[0-9]$")){
        // 如果字符串的最后是身份证号，直接替换即可
        if(highIndex < http_cookie.length){
          // 拿到身份证号的最后一个字符
          val highLetter = http_cookie.charAt(highIndex).toString
          // 后一位也不是数字，说明这个字符串就是一个身份证号
          if(!highLetter.matches("^[0-9]$")){
            // 直接替换
            cookie = cookie.replace(idMatcher.group(),md5.getMD5ofStr(idMatcher.group()))
          }
        }else {
          cookie = cookie.replace(idMatcher.group(), md5.getMD5ofStr(idMatcher.group()))
        }
      }
    }
    cookie
  }
}
