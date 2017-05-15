package sparkToDB

import java.sql.{Connection, DriverManager, PreparedStatement}

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
/**
  * Created by cluster on 2016/12/10.
  * 将kafka中，topic为weibo_content2中的数据写入Mysql中
  */
object sparkMySQL_weibo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("weibo")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    /**
      * kafka配置
      */
    val topics = Set("weibo_content2")
    val brokers = "192.168.31.6:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder","auto.offset.reset" -> "smallest")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val rdd = kafkaStream.map(json => {
      val jsonStr = json._2
      val jsonObj = JSONObject.fromObject(jsonStr)
      (jsonObj.getString("user_fansNum"),jsonObj.getString("text").replaceAll("[\\x{10000}-\\x{10FFFF}]", ""),jsonObj.getString("user_header"),jsonObj.getString("created_timestamp"),jsonObj.getString("address"),jsonObj.getString("user_gender"),jsonObj.getString("user_name"),jsonObj.getString("user_verified"))
    })

    rdd.foreachRDD(rdd => {
      rdd.foreachPartition(iterator => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql: String = "INSERT INTO weibo_content2 (user_fansNum,text,user_header,created_timestamp,address,user_gender,user_name,user_verified,emotion) VALUES (?,?,?,?,?,?,?,?,?)"

        try {
          conn = DriverManager.getConnection("jdbc:mysql://192.168.31.7:3306/qczjtest?useUnicode=true&characterEncoding=UTF-8", "root", "dp12345678")
          iterator.foreach(line=>{
            ps = conn.prepareStatement(sql)
            ps.setString(1,line._1)
            ps.setString(2, line._2)
            ps.setString(3,line._3)
            ps.setInt(4, line._4.toInt)
            ps.setString(5,line._5)
            ps.setString(6,line._6)
            ps.setString(7,line._7)
            ps.setString(8,line._8)
            ps.setString(9,"")
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
