/**JavaWordCount
 * 
 * com.magicstudio.spark
 *
 * SparkWordCount.java
 *
 * dumbbellyang at 2016年8月3日 下午9:33:05
 *
 * Mail:yangdanbo@163.com Weixin:dumbbellyang
 *
 * Copyright 2016 MagicStudio.All Rights Reserved
 */
package sparkSeg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import scala.Tuple2;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SparkWordCount implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = -6629178988243085024L;
	
	private String doc = "唐诗三百首";
    private boolean isSelectFile = false;
    private int wordLength = 0; //0 为所有单词长度
    
    //conf 和 sc变量 必须声明为静态类或者在静态方法如main方法中调用
    private static SparkConf conf = null;
    private static JavaSparkContext sc = null;


    private static String sql = null;
    private static DBHelper db1 = null;
    private static ResultSet ret = null;

    private void initSpark(){
    	/**
         * 1、创建SparkConf对象，设置Spark应用程序的配置信息
         */
        //SparkConf conf = new SparkConf()
        conf = new SparkConf()
                //设置Spark应用程序的名称
                .setAppName(SparkWordCount.class.getSimpleName());
        
        conf.setMaster("local");
        /**
         * 2、创建SparkContext对象，Java开发使用JavaSparkContext；Scala开发使用SparkContext
         * 在Spark中，SparkContext负责连接Spark集群，创建RDD、累积量和广播量等。
         * Master参数是为了创建TaskSchedule（较低级的调度器，高层次的调度器为DAGSchedule），如下：
         *         如果setMaster("local")则创建LocalSchedule；
         *         如果setMaster("spark")则创建SparkDeploySchedulerBackend。
         *         在SparkDeploySchedulerBackend的start函数，会启动一个Client对象，连接到Spark集群。
         */
        //JavaSparkContext sc = new JavaSparkContext(conf);
        sc = new JavaSparkContext(conf);
    }
    
	SparkWordCount(String doc, boolean isSelectFile, int wordLength){
		this.doc = doc;
		this.isSelectFile = isSelectFile;
		this.wordLength = wordLength;
		
		initSpark();
	}
	
	SparkWordCount(){
		initSpark();
	}
	
	private List<String> getSplitWords(String line){
		List<String> words = new ArrayList<String>();
		if (line == null || line.trim().length() == 0){
			return words;
		}
		
		try {
            //把stopword.dic和IKAnalyzer.cfg直接拖到src的下级目录中，就可以使用去掉停用词了
			InputStream is = new ByteArrayInputStream(line.getBytes("UTF-8"));
            //false代表最细粒度切分
			IKSegmenter seg = new IKSegmenter(new InputStreamReader(is),false);
			
			Lexeme lex = seg.next();
			
			while (lex != null){
				String word = lex.getLexemeText();
				if (wordLength == 0 || word.length() == wordLength){
					words.add(word);
				}
				
				lex = seg.next();
			}
		
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return words;
	}
	
	public JavaPairRDD<String, Integer> wordCount(){
        /**
         * 3、sc中提供了textFile方法是SparkContext中定义的，如下：
         *         def textFile(path: String): JavaRDD[String] = sc.textFile(path)    
         * 用来读取HDFS上的文本文件、集群中节点的本地文本文件或任何支持Hadoop的文件系统上的文本文件，
         * 它的返回值是JavaRDD[String]，是文本文件每一行
         */
        //JavaRDD<String> lines = sc.textFile("hdfs://soy1:9000/mapreduces/word.txt");
        JavaRDD<String> lines = null;
        if (isSelectFile){
        	lines = sc.textFile(doc);
        }
        else{
//        	lines = sc.textFile("src/com/magicstudio/spark/text/" + doc + ".txt");
            //微博，汽车之家分词
            lines = sc.textFile("segmentition/src/sparkSeg/text/Sum.txt");

        }
        
        /**
         * 4、将行文本内容拆分为多个单词
         * lines调用flatMap这个transformation算子（参数类型是FlatMapFunction接口实现类）
         * 返回每一行的每个单词
         * 加入了中文分词的功能，调用分词后的list结果
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>(){
            private static final long serialVersionUID = -3243665984299496473L;

            public Iterable<String> call(String line) throws Exception {
                //return Arrays.asList(line.split("\t"));
            	return getSplitWords(line);
            }
        });
        
        /**
         * 5、将每个单词的初始数量都标记为1个
         * words调用mapToPair这个transformation算子（参数类型是PairFunction接口实现类，PairFunction<String, String, Integer>的三个参数是<输入单词, Tuple2的key, Tuple2的value>），返回一个新的RDD，即JavaPairRDD
         */
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -7879847028195817507L;

            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        
        /**
         * 6、计算每个相同单词出现的次数
         * pairs调用reduceByKey这个transformation算子（参数是Function2接口实现类）对每个key的value进行reduce操作，返回一个JavaPairRDD，这个JavaPairRDD中的每一个Tuple的key是单词、value则是相同单词次数的和
         */
        JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -4171349401750495688L;

            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        
        return wordCount;
	}
	
	public JavaPairRDD<String, Integer> sortByValue(JavaPairRDD<String, Integer> wordCount, boolean isAsc){
		//added by Dumbbell Yang at 2016-08-03
        //加入按词频排序功能
        //先把key和value交换，然后按sortByKey，最后再交换回去
        JavaPairRDD<Integer, String> pairs2 = wordCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            private static final long serialVersionUID = -7879847028195817508L;

            public Tuple2<Integer, String> call(Tuple2<String, Integer> word) throws Exception {
                return new Tuple2<Integer, String>(word._2, word._1);
            }
        });
        
        //降序
        pairs2 = pairs2.sortByKey(isAsc);
        
        //再次交换key和value
        wordCount = pairs2.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            private static final long serialVersionUID = -7879847028195817509L;

            public Tuple2<String, Integer> call(Tuple2<Integer, String> word) throws Exception {
                return new Tuple2<String, Integer>(word._2, word._1);
            }
        });
        
        return wordCount;
	}
	
	public void closeSpark(JavaPairRDD<String, Integer> wordCount) {
        /**
         * 7、使用foreach这个action算子提交Spark应用程序
         * 在Spark中，每个应用程序都需要transformation算子计算，最终由action算子触发作业提交
         */
        wordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -5926812153234798612L;

            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                //SQL语句
                sql = "INSERT INTO segword (keyword, num) VALUES (?,?)";

                String one = wordCount._1;
                int two = wordCount._2;
                //String text = wordCount._1 + ":" + wordCount._2;
                //获取字符串的字节，一个汉字三个字节
                byte[] buff=one.getBytes();
                int f=buff.length;
                //过滤掉字节数小于等于3的词
                if ( f > 3){
                    String text = one + ":" + two;
                    try {
                        //创建DBHelper对象
                        //向数据库中插入值
                        db1 = new DBHelper(sql);
                        db1.pst.setString(1,one);
                        db1.pst.setInt(2,two);
                        db1.pst.executeUpdate();
//                        ret.close();
//                        db1.close();//关闭连接
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }finally {
                        if (db1.pst != null)
                            db1.pst.close();
                        if (db1.conn != null)
                            db1.conn.close();
                    }
                    //将结果写入文件中
//                    try {
//                        String path = "segmentition/src/sparkSeg/text/cipin.txt";
//                        File file = new File(path);
//                        if (!file.exists())
//                            file.createNewFile();
//                        FileOutputStream out = new FileOutputStream(file, true); //如果追加方式用true
//                        StringBuffer sb = new StringBuffer();
//                        sb.append(text + "\n");
//                        out.write(sb.toString().getBytes("utf-8"));//注意需要转换对应的字符集
//                        out.close();
//                    } catch (IOException ex) {
//                        System.out.println(ex.getStackTrace());
//                    }

                    System.out.println(text);
                }
                   //System.out.println(wordCount._1 + "("+f + ")"+ ":" + wordCount._2);
            }
        });

        /**
         * 8、将计算结果文件输出到文件系统
         *         HDFS：
         *             使用新版API（org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;）
         *                 wordCount.saveAsNewAPIHadoopFile("hdfs://ns1/spark/wordcount", Text.class, IntWritable.class, TextOutputFormat.class, new Configuration());
         *             使用旧版API（org.apache.hadoop.mapred.JobConf;org.apache.hadoop.mapred.OutputFormat;）
         *                 wordCount.saveAsHadoopFile("hdfs://ns1/spark/wordcount", Text.class, IntWritable.class, OutputFormat.class, new JobConf(new Configuration()));
         *             使用默认TextOutputFile写入到HDFS(注意写入HDFS权限，如无权限则执行：hdfs dfs -chmod -R 777 /spark)
         *                 wordCount.saveAsTextFile("hdfs://soy1:9000/spark/wordCount");
         */
        //wordCount.saveAsTextFile("hdfs://soy1:9000/spark/wordCount");

        /**
         * 9、关闭SparkContext容器，结束本次作业
         */
        sc.close();
    }
	public static void main(String[] args) {
		// TODO Auto-generated method stub
        SparkWordCount app = new SparkWordCount();
        
        JavaPairRDD<String, Integer> wordCount = app.wordCount();
        
        wordCount = app.sortByValue(wordCount, false);
        
        app.closeSpark(wordCount);
	}

}
