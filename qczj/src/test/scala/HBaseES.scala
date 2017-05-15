import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by seagle on 6/28/16.
  * spark sql 查询 HBase中数据
  *
  */
object HBaseES {
  //过滤日志
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]): Unit ={
    // 本地模式运行,便于测试
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HBaseTest")
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableInputFormat.INPUT_TABLE,"autohome_koubei")
    hBaseConf.set("hbase.zookeeper.quorum","192.168.31.61,192.168.31.62,192.168.31.63")
    //设置zookeeper连接端口，默认2181
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    // 创建 spark context
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    val shop = hbaseRDD.map(r=>(
      Bytes.toString(r._2.getValue(Bytes.toBytes("qczj"),Bytes.toBytes("membername"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("qczj"),Bytes.toBytes("content"))),
        Bytes.toString(r._2.getValue(Bytes.toBytes("qczj"),Bytes.toBytes("emotion")))
      )).toDF("membername","content","emotion")
    shop.registerTempTable("autohome_koubei")
    // 测试
    val df2 = sqlContext.sql("SELECT membername,emotion FROM autohome_koubei")
    df2.foreach(println)
    sc.stop()

  }
}