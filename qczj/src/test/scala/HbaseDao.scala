
import java.util.regex.{Matcher, Pattern}

import analysis.Analysis
import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HbaseDao {

  //过滤日志
  Logger.getLogger("org").setLevel(Level.ERROR)
  //hbase参数配置
  val hbaseconf = new Configuration()
  hbaseconf.set("hbase.zookeeper.quorum", "192.168.31.61,192.168.31.62,192.168.31.63")
  hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseconf.set("zookeeper.znode.parent", "/hbase-unsecure")
  val hTablePool = HConnectionManager.createConnection(hbaseconf)
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
    *
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

  //插入操作
  def insert(tname: String, rowkey: String, family: String, quailifer: String, value: String) {
    val htable = hTablePool.getTable(tname)
    htable.setAutoFlush(false)
    htable.setWriteBufferSize(5 * 1024 * 1024)
    try {
      val put = new Put(rowkey.getBytes)
      put.setWriteToWAL(false)
      put.add(family.getBytes, quailifer.getBytes, value.getBytes)

      htable.put(put)
    } catch {
      case ex: Exception => ex.getMessage
    } finally {
      htable.close()
    }
  }

  def insertBitch(htable: HTableInterface, tname: String, rowkey: String, family: String, quailifer: String, value: String) = {
    try {
      val put = new Put(rowkey.getBytes)
      //      put.setWriteToWAL(true)
      put.add(family.getBytes, quailifer.getBytes, value.getBytes)
      htable.put(put)
    } catch {
      case ex: Exception => ex.getMessage
    } finally {
      htable.close()
    }
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("weibo")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topics = Set("weibo_content2")
    val brokers = "192.168.31.6:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder","auto.offset.reset" -> "smallest")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //获取json类型原始数据中，我们要的指定的字段数据
    val rdd = kafkaStream.map(json => {
      val jsonStr = json._2
      val jsonObj = JSONObject.fromObject(jsonStr)
      //对从json串中获取的user_fansNum进行过滤操作，将“万”转换为阿拉伯数字
      val fansnum = user_fansNum_alis(jsonObj.getString("user_fansNum"))
      //情感字段emotion根据text字段来判断
      val emotion_str = Analysis_emotion(jsonObj.getString("text").replaceAll("[\\x{10000}-\\x{10FFFF}]", ""))
      (fansnum,jsonObj.getString("text").replaceAll("[\\x{10000}-\\x{10FFFF}]", ""),jsonObj.getString("user_header"),jsonObj.getString("created_timestamp"),jsonObj.getString("address"),jsonObj.getString("user_gender"),jsonObj.getString("user_name"),jsonObj.getString("user_verified"),emotion_str)
    })
    rdd.foreachRDD(line =>{
      line.foreachPartition( iter =>{
        iter.foreach( line =>{
          insert("weibo_text",line._4, "wb", "user_fansNum", line._1)
          insert("weibo_text",line._4, "wb", "text", line._2)
          insert("weibo_text",line._4, "wb", "user_header", line._3)
          insert("weibo_text",line._4, "wb", "address", line._5)
          insert("weibo_text",line._4, "wb", "user_gender", line._6)
          insert("weibo_text",line._4, "wb", "user_name", line._7)
          insert("weibo_text",line._4, "wb", "user_verified", line._8)
          insert("weibo_text",line._4, "wb", "emotion", line._9)
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}