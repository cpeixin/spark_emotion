package sparkToDB


/**
  * Created by cluster on 2016/12/10.
  */
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by cluster on 2016/12/10.
  */
object sparkMySQL_purchase {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("purchase")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    /**
      * kafka配置
      */
    val topics = Set("autohome_purchase_goal")
    //    val topic2 = Set("autohome_purchase_goal")

    val brokers = "192.168.31.6:9092"

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder","auto.offset.reset" -> "smallest")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    kafkaStream.foreach(rdd => {
      rdd.foreach(msg => {
        println(msg._2)
      })
    })


    ssc.start()
    //等待
    ssc.awaitTermination()
  }
}