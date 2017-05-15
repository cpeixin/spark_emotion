import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.HashPartitioner
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.slf4j.LoggerFactory
/**
  * Created by DN on 2016/11/22.
  * 更新wordcount
  */
object UpdateWordCount {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val updateFunction = (iter: Iterator[(String,Seq[Int],Option[Int])]) => {
    //将iter中的历史次数和现有的数据叠加，然后将单词和出现的最后次数输出
    //iter.flatMap(t=>Some(t._2.sum + t._3.getOrElse(0)).map(x=>(t._1,x)))
    iter.flatMap{case(x,y,z)=>Some(y.sum+z.getOrElse(0)).map(v =>(x,v))}
  }
  def main(args: Array[String]) {
    //创建
    val conf = new SparkConf().setMaster("local[4]").setAppName("QCZJ_Spark_UpdateWordCount")
    val ssc = new StreamingContext(conf,Seconds(5))
    //回滚点设置在本地
    ssc.checkpoint(".")

    //将回滚点写到hdfs,但是运行没有权限
    //ssc.checkpoint("hdfs://192.168.31.60:8020/check")
    val lines = ssc.socketTextStream("192.168.31.6", 8888)
    /**
      * updateStateByKey()更新数据
      * 1、更新数据的具体实现函数
      * 2、分区信息
      * 3、boolean值
      */
    val results = lines.flatMap(_.split("，")).map((_,1)).updateStateByKey(updateFunction,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
