package com.xuehai.utils

import java.text.SimpleDateFormat
import java.util.Date

import com.dingtalk.api.DefaultDingTalkClient
import com.dingtalk.api.request.OapiRobotSendRequest

import scala.collection.JavaConverters._

/**
  * Created by Administrator on 2019/5/23 0023.
  */
object Utils extends Constants{

    /**
      * 钉钉机器人消息推送
      *
      * @param user "all"-@所有人，"18810314189,13724612033"-@指定的人，以逗号分割
      * @param massage 推送的消息
      */
    def dingDingRobot(user: String, massage: String): Unit ={
        val text = new OapiRobotSendRequest.Text()
        text.setContent(massage)

        val at = new OapiRobotSendRequest.At()
        if(user=="all"){
            at.setIsAtAll("true")
        }else{
            at.setAtMobiles(user.split(",").toList.asJava)
        }

        val request = new OapiRobotSendRequest()
        request.setMsgtype("text")
        request.setText(text)
        request.setAt(at)

        val client = new DefaultDingTalkClient(DingDingUrl)
        client.execute(request)
    }

    /**
      * 毫秒数生产日期和小时，例如1559615598000-》(20190604, 10)
      *
      * @param x long
      * @return (day, hour)
      */
    def str2Day(x: String): String ={
        val date = new Date(x.toLong)
        val dateFormat = new SimpleDateFormat("yyyyMMdd")
        val dateStr = dateFormat.format(date).split(" ")

        dateStr(0)
    }

   //如果字符中中有学生就删除
   def remove(students: String,student: String): String ={
       val strings = students.split(",")
       val str = strings(0)
       if(strings.length>1){
           if(str==student){
               students.replace(student+",", "")
           } else {
               students.replace(","+student, "")
           }
       }
       else {
           students.replace(student, "")
       }




   }


  /**
    * null 转"null"，非空则直接转String，防止空指针异常
    *
    * @return
    */
  def null2Str(x: Any): String ={
    if(x == null) return ""
    else x.toString
  }

    /**
      * null 转0，计算kafka的offset使用
      *
      * @param x
      * @return
      */
    def null20(x: Any): Int ={
        if(x == null) return 0
        else x.toString.toInt
    }


  def doubletoint(ss:String)={
    val a="1.0000"
    val b="0.0000"
    var sss=ss
    if(ss==a||ss==b){
      sss= ss.substring(0,1)
    }
    sss
  }


  def main(args: Array[String]) {
        val str = str2Day("1559615598000")

        println(str)
    }
}
