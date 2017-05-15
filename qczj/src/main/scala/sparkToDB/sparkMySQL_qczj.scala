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
  * 将kafka中，topic为autohome_koubei中的数据写入Mysql中
  */
object sparkMySQL_qczj {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("qczj")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql: String = "INSERT INTO autohome_koubei (reportdate,membername,content,emotion) VALUES (?,?,?,?)"
    /**
      * kafka配置
      */
    val topics = Set("autohome_koubei")
    val brokers = "192.168.31.6:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder","auto.offset.reset" -> "smallest")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val rdd = kafkaStream.map(json => {
      val jsonStr = json._2
      val jsonObj = JSONObject.fromObject(jsonStr).getString("result")
      val jsonObj_result = JSONObject.fromObject(jsonObj)
      val emotion = "中性"
      (jsonObj_result.getString("reportdate"),jsonObj_result.getString("membername"),jsonObj_result.getString("content").replaceAll("[\\x{10000}-\\x{10FFFF}]", ""),emotion)
    })
    rdd.foreachRDD(rdd => {
      rdd.foreachPartition(iterator => {
        try {
          conn = DriverManager.getConnection("jdbc:mysql://192.168.31.7:3306/qczjtest?useUnicode=true&characterEncoding=UTF-8", "root", "dp12345678")
          iterator.foreach(line=>{
            ps = conn.prepareStatement(sql)
            ps.setString(1,line._1)
            ps.setString(2, line._2)
            ps.setString(3, line._3)
            ps.setString(4,line._4)
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
