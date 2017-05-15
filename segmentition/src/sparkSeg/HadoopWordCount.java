/**JavaWordCount
 * 
 * com.magicstudio.spark
 *
 * HadoopWordCount.java
 *
 * dumbbellyang at 2016年8月4日 下午9:45:54
 *
 * Mail:yangdanbo@163.com Weixin:dumbbellyang
 *
 * Copyright 2016 MagicStudio.All Rights Reserved
 */
package sparkSeg;

//Java class
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Map;

//hadoop class
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

//word split class
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class HadoopWordCount {

    private static SortableMap<Integer> totalWords = new SortableMap<Integer>();
    
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private String pattern = "[^//w]"; // 正则表达式，代表不是0-9, a-z,
											// A-Z的所有其它字符,其中还有下划线

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().toLowerCase(); // 全部转为小写字母
			/*
			 * line = line.replaceAll(pattern, " "); // 将非0-9, a-z, A-Z的字符替换为空格
			 * StringTokenizer itr = new StringTokenizer(line); while
			 * (itr.hasMoreTokens()) { word.set(itr.nextToken());
			 * context.write(word, one); }
			 */

			try {
				InputStream is = new ByteArrayInputStream(
						line.getBytes("UTF-8"));
				IKSegmenter seg = new IKSegmenter(new InputStreamReader(is),
						false);

				Lexeme lex = seg.next();

				while (lex != null) {
					String text = lex.getLexemeText();
					word.set(text);
					context.write(word, one);
					// output.collect(word, one);
					
					lex = seg.next();
				}

			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
			//保存结果
			totalWords.put(key.toString(), Integer.valueOf(sum));
		}
	}

	private static class IntWritableDecreasingComparator extends
			IntWritable.Comparator {
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	public static Map<String, Integer> wordCount(String doc, boolean isSelectFile, int wordLength) throws IOException{
		Configuration conf = new Configuration();
		Path inputFile; //word count job input
		if (isSelectFile){
			inputFile = new Path(doc);
		}
		else{
			inputFile = new Path("src/com/magicstudio/spark/text/" + doc + ".txt"); 
		}

		//hadoop job会自动创建输出文件夹，所以必须确保输出文件夹不存在
        FileUtil.deleteFolder("C:\\hadoop");
        FileUtil.deleteFolder("C:\\hadoopsort");
		Path outputFolder = new Path("C:\\hadoop");//word count job output,sort job input
		Path sortOutput = new Path("C:\\hadoopsort");//sort job output
		
		try {
			Job job = new Job(conf, "word count");
			job.setJarByClass(HadoopWordCount.class);
			
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, inputFile);
			FileOutputFormat.setOutputPath(job, outputFolder);// 先将词频统计任务的输出结果写到临时目
			// 录中, 下一个排序任务以临时目录为输入目录。
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (job.waitForCompletion(true)) {
				Job sortJob = new Job(conf, "sort");
				sortJob.setJarByClass(HadoopWordCount.class);

				FileInputFormat.addInputPath(sortJob, outputFolder);
				sortJob.setInputFormatClass(SequenceFileInputFormat.class);

				/* InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换 */
				sortJob.setMapperClass(InverseMapper.class);
				/* 将 Reducer 的个数限定为1, 最终输出的结果文件就是一个。 */
				sortJob.setNumReduceTasks(1);
				FileOutputFormat.setOutputPath(sortJob, sortOutput);

				sortJob.setOutputKeyClass(IntWritable.class);
				sortJob.setOutputValueClass(Text.class);
				/*
				 * Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。 因此我们实现了一个
				 * IntWritableDecreasingComparator 类,　 并指定使用这个自定义的 Comparator
				 * 类对输出结果中的 key (词频)进行排序
				 */
				sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);

			    if (sortJob.waitForCompletion(true)){
			    	//added by Dumbbell Yang at 2016-08-07
			    	if (wordLength == 0){
			    		return totalWords.sortMapByValue(false);
			    	}
			    	else{
			    		SortableMap<Integer> words = new SortableMap<Integer>();
			    		for(String key:totalWords.keySet()){
			    			if (key.length() == wordLength){
			    				words.put(key, totalWords.get(key));
			    			}
			    		}
			    		
			    		return words.sortMapByValue(false);
			    	}
			    }
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			FileSystem.get(conf).deleteOnExit(outputFolder);
		}
		
		return null;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		/*String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Path tempDir = new Path("wordcount-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE))); // 定义一个临时目录
        */
		//Path inputFile = new Path("E:\\text\\唐诗三百首.txt"); //word count job input
		Path inputFile = new Path("src/com/magicstudio/spark/text/唐诗三百首.txt");
		Path outputFolder = new Path("E:\\text\\hadoop");//word count job output,sort job input
		Path sortOutput = new Path("E:\\text\\hadoopsort");//sort job output
		
		Job job = new Job(conf, "word count");
		job.setJarByClass(HadoopWordCount.class);
		try {
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, inputFile);
			FileOutputFormat.setOutputPath(job, outputFolder);// 先将词频统计任务的输出结果写到临时目
			// 录中, 下一个排序任务以临时目录为输入目录。
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (job.waitForCompletion(true)) {
				Job sortJob = new Job(conf, "sort");
				sortJob.setJarByClass(HadoopWordCount.class);

				FileInputFormat.addInputPath(sortJob, outputFolder);
				sortJob.setInputFormatClass(SequenceFileInputFormat.class);

				/* InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换 */
				sortJob.setMapperClass(InverseMapper.class);
				/* 将 Reducer 的个数限定为1, 最终输出的结果文件就是一个。 */
				sortJob.setNumReduceTasks(1);
				FileOutputFormat.setOutputPath(sortJob, sortOutput);

				sortJob.setOutputKeyClass(IntWritable.class);
				sortJob.setOutputValueClass(Text.class);
				/*
				 * Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。 因此我们实现了一个
				 * IntWritableDecreasingComparator 类,　 并指定使用这个自定义的 Comparator
				 * 类对输出结果中的 key (词频)进行排序
				 */
				sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);

			    System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
			}
		} finally {
			FileSystem.get(conf).deleteOnExit(outputFolder);
		}
	}
}
