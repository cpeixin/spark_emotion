package sparkToDB

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.regex.{Matcher, Pattern}

import analysis.Analysis
import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
/**
  * Created by cluster on 2016/12/10.
  * 将kafka中，topic为weibo_content2中的数据写入Mysql中
  * 并且加入情感分析
  */
object sparkMySQL_weibo_emotion {

  Logger.getLogger("org").setLevel(Level.ERROR)

  /**
    * user_fansNum粉丝数清洗
    * 清洗规则：去掉 例如：423万后面的万字，并且将数值变成4230000
    * @param fansNumStr
    * @return user_fansNum
    */
  def user_fansNum_alis(fansNumStr: String): String = {
    var user_fansNum = ""
    val p: Pattern = Pattern.compile("\\d+")
    val str: Array[String] = p.split(fansNumStr)
    if (str.length == 2) {
      val reg = "[\u4e00-\u9fa5]"
      val p1: Pattern = Pattern.compile(reg)
      val m: Matcher = p1.matcher(fansNumStr)
      val repickStr = m.replaceAll("")
      user_fansNum = repickStr + "0000"
    } else {
      user_fansNum = fansNumStr
    }
    user_fansNum
  }

  /**
    * 情感分析方法
    * @param str
    * @return  emotion
    */
  def Analysis_emotion(str: String): String ={
    //创建情感分析实例
    val analysis: Analysis = new Analysis()
    var emotion: String = ""
    val em: Int = analysis.parse(str).getCode
    //根据分析之后的值 1 0 -1 去匹配情感值
    if (em == 1) {
      emotion = "正面"
    }
    else if (em == 0) {
      emotion = "中性"
    }
    else {
      emotion = "负面"
    }
    emotion
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("weibo")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    //kafka配置
    val topics = Set("weibo_content2")
    val brokers = "192.168.31.6:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder","auto.offset.reset" -> "smallest")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //获取json类型原始数据中，我们要的制定字段数据
    val rdd = kafkaStream.map(json => {
      val jsonStr = json._2
      val jsonObj = JSONObject.fromObject(jsonStr)
      //对从json串中获取的user_fansNum进行过滤操作，将“万”转换为阿拉伯数字
      val fansnum = user_fansNum_alis(jsonObj.getString("user_fansNum"))
      //情感字段emotion根据text字段来判断
      val emotion_str = Analysis_emotion(jsonObj.getString("text").replaceAll("[\\x{10000}-\\x{10FFFF}]", ""))
      (fansnum,jsonObj.getString("text").replaceAll("[\\x{10000}-\\x{10FFFF}]", ""),jsonObj.getString("user_header"),jsonObj.getString("created_timestamp"),jsonObj.getString("address"),jsonObj.getString("user_gender"),jsonObj.getString("user_name"),jsonObj.getString("user_verified"),emotion_str)
    })
    rdd.foreachRDD(rdd => {
      rdd.foreachPartition(iterator => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql: String = "INSERT INTO weibo_content2_emotion (user_fansNum,text,user_header,created_timestamp,address,user_gender,user_name,user_verified,emotion) VALUES (?,?,?,?,?,?,?,?,?)"
        try {
          conn = DriverManager.getConnection("jdbc:mysql://192.168.31.7:3306/qczjtest?useUnicode=true&characterEncoding=UTF-8", "root", "dp12345678")
          iterator.foreach(line=>{
            ps = conn.prepareStatement(sql)
            ps.setString(1,line._1)
            ps.setNString(2, line._2)
            ps.setString(3,line._3)
            ps.setInt(4, line._4.toInt)
            ps.setString(5,line._5)
            ps.setString(6,line._6)
            ps.setString(7,line._7)
            ps.setString(8,line._8)
            ps.setString(9,line._9)
            ps.executeUpdate()
          })
        } catch {
          case e: Exception =>  println(e.printStackTrace())
        }finally {
          if (ps != null)
            ps.close()
          if (conn != null)
            conn.close()
        }
      })
    })
    ssc.start()
    //等待
    ssc.awaitTermination()
  }
}
