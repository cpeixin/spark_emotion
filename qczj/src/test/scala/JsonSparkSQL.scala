import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by cluster on 2016/11/28.
  * spark sql 操作json文件
  */
object JsonSparkSQL {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var conf = new SparkConf().setAppName("Test").setMaster("local[2]")
    var sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val skuDF = sqlContext.read.json("skujson.json")
    skuDF.registerTempTable("sku")

    val skuDF_1 = sqlContext.sql("select * from sku")
    skuDF_1.show()

    //skuDF.show();//展示所有信息
    //skuDF.printSchema();//显示数据的结构

  }
}
