import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, HTable}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, _}


/**
  * spark 连接 HBase
  */

object SparkHBase_1 extends Serializable {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HBaseTest").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)

    var table_name = "autohome_koubei"
    val conf = HBaseConfiguration.create()
    // 配置项都在hbase-site.xml中
    conf.set("hbase.rootdir", "hdfs://192.168.31.60:8020/apps/hbase/data")
    conf.set("hbase.zookeeper.quorum", "192.168.31.61,192.168.31.62,192.168.31.63")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
//    conf.set("hbase.master", "60001")
    conf.set("hbase.master", "16000")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFormat.INPUT_TABLE, table_name)

    val hadmin = new HBaseAdmin(conf)

    //创建表操作
//    if (!hadmin.isTableAvailable("autohome_koubei")) {
//      print("Table Not Exists! Create Table")
//      val tableDesc = new HTableDescriptor("autohome_koubei")
//      tableDesc.addFamily(new HColumnDescriptor("qczj".getBytes()))
//      hadmin.createTable(tableDesc)
//    }else{
//      print("Table  Exists!  not Create Table")
//    }
//
//    val table = new HTable(conf, "autohome_koubei")
//    for (i <- 1 to 5) {
//      var put = new Put(Bytes.toBytes("row" + i))
//      put.add(Bytes.toBytes("basic"), Bytes.toBytes("name"), Bytes.toBytes("value " + i))
//      table.put(put)
//    }
//    table.flushCommits()


    val table = new HTable(conf, "autohome_koubei")
    //Scan操作
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hbaseRDD.count()
    println("HBase RDD Count:" + count)
    hbaseRDD.cache()

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val g = new Get("2016-11-21".getBytes)
    val result = table.get(g)
    val value = Bytes.toString(result.getValue("qczj".getBytes,"content".getBytes))
    println("2016-11-21 内容为:"+value)


    hbaseRDD.cache()
//    print("------------------------scan----------")
//    val res = hbaseRDD.take(count.toInt)
//    for (j <- 1 until count.toInt) {
//      println("j: " + j)
//      var rs = res(j - 1)._2
//      var kvs = rs.raw
//      for (kv <- kvs)
//        println("rowkey:" + new String(kv.getRow()) +
//          " cf:" + new String(kv.getFamily()) +
//          " column:" + new String(kv.getQualifier()) +
//          " value:" + new String(kv.getValue()))
//    }

    /*    println("-------------------------")
        println("--take1" + hBaseRDD.take(1))
        println("--count" + hBaseRDD.count())*/


    //insert_hbase(100002,3)
  }
  //写入hbase
    /* def insert_hbase(news_id:Int,type_id:Int): Unit ={
	     var table_name = "news"
	     val conf = HBaseConfiguration.create()
	     conf.set("hbase.zookeeper.quorum","192.168.110.233, 192.168.110.234, 192.168.110.235");
	     conf.set("hbase.zookeeper.property.clientPort", "2181");
	     val table = new HTable(conf, table_name)
	     val hadmin = new HBaseAdmin(conf)
	     val p = new Put(row)
	     p.add(Bytes.toBytes("content"),Bytes.toBytes("typeid"),Bytes.toBytes(type_id.toString()))
	     table.put(p)
	     table.close()
	   } */
   }