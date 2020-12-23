import java.sql.ResultSet
import java.text.DecimalFormat

import com.alibaba.fastjson.JSON
import com.xuehai.utils.MysqlUtils.{conn, getMysqlConnection}

object ss {
  def main(args: Array[String]): Unit = {

    println(doubletoint("1.0000"))
  }

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

  def doubletoint(ss:String)={
    val a="1.0000"
    val b="0.0000"
    var sss=ss
    if(ss==a||ss==b){
      sss= ss.substring(0,1)
    }
    sss
  }

}
