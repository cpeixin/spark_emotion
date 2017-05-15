
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by DN on 2016/11/22.
  * wordcount
  */
object SparkWordCount {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //开本地线程两个处理
    val conf = new SparkConf().setMaster("local[2]").setAppName("QCZJ_Spark_WordCount")
    //每隔5秒计算一批数据
    val ssc = new StreamingContext(conf, Seconds(5))
    //监控机器ip为192.168.31.6:8888端号的数据,8888 -l 9999，否则会报错,但进程不会中断
    val lines = ssc.socketTextStream("192.168.31.6", 8888)
    //按","切分输入数据
    val words = lines.flatMap(_.split("，"))
    //计算wordcount
    val pairs = words.map(word => (word, 1))
    //word ++
    val wordCounts = pairs.reduceByKey(_ + _)
    //排序结果集打印，先转成rdd，然后排序true升序，false降序，可以指定key和value排序_._1是key，_._2是value
    val sortResult=wordCounts.transform(rdd=>rdd.sortBy(_._2,false))
    // print操作会将DStream每一个batch中的前10个元素在driver节点打印出来
    sortResult.print()
    ssc.start()             // 开启计算
    ssc.awaitTermination()  // 阻塞等待计算

  }
}
