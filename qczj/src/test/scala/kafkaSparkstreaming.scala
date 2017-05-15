import java.io.{PrintWriter, File}
import java.sql.DriverManager
import com.mysql.jdbc.{PreparedStatement, Connection}
import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.Seconds
/**
  * Created by cluster on 2016/11/28.
  * 打印kafka中的数据
  */
object kafkaSparkstreaming {
  //Logger.getLogger("org").setLevel(Level.ERROR)
//  logInfo("test log")
  def main(args: Array[String]) {
    /**
      * Director
      */
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
//    val ssc = new StreamingContext(conf, Durations.seconds(5))
    val ssc = new StreamingContext(conf,Seconds(5))
//    val topics = Set("skurawdataonline_new01")
    val topics = Set("skurawdataonline_new01")
    val brokers = "process1.pd.dp:6667,process6.pd.dp:6667,process7.pd.dp:6667"
//    var count = 0
//    var conn: Connection = null
//    var ps: PreparedStatement = null
//    val sql: String = "INSERT INTO autohome_koubei_1 (reportdate,membername,content) VALUES (?,?,?)"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder","auto.offset.reset" -> "smallest")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    /**
      * 方法二
      */
    kafkaStream.foreachRDD(rdd => {
      rdd.foreach( msg=>{
        if (msg._2.get){

        }

      })
    })
    ssc.start()
    //等待
    ssc.awaitTermination()
    //ssc.stop()
  }
}
