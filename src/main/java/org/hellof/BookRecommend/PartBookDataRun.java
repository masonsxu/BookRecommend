package org.hellof.BookRecommend;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hellof.BookRecommend.HdfsDao.HdfsDao;

public class PartBookDataRun {
	public static class PartBookDataMapper extends Mapper<Object, Text, IntWritable, Text>{
		private final static IntWritable k = new IntWritable();
	    private final static Text v = new Text();
	    
	    @Override
	    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	String[] tokens = BookRecommendMain.DELIMITER.split(value.toString()); //建立用户读书列表

	        if (tokens.length == 3) {
	        	int userID = Integer.parseInt(tokens[0]);  //获得用户ID
		        //System.out.println(userID);
		        String itemID = tokens[1]; //获得图书名称
		        //System.out.println(itemID);
		        String pref = tokens[2]; //获得用户借阅此书的借阅次数
	            //System.out.println(pref);
	            k.set(userID);
	            v.set(itemID + ":" + pref);
		        context.write(k, v);
			}else if(tokens.length == 4){
				int userID = Integer.parseInt(tokens[0]);  //获得用户ID
		        //System.out.println(userID);
		        String itemID = tokens[1] + "·" + tokens[2]; //获得图书名称
		        //System.out.println(itemID);
		        String pref = tokens[3]; //获得用户借阅此书的借阅次数
	            //System.out.println(pref);
	            k.set(userID);
	            v.set(itemID + ":" + pref);
		        context.write(k, v);
			}else {
				int userID = Integer.parseInt(tokens[0]);  //获得用户ID
		        //System.out.println(userID);
		        String itemID = tokens[1] + "·" + tokens[2] + "·" + tokens[3]; //获得图书名称
		        //System.out.println(itemID);
		        String pref = tokens[4]; //获得用户借阅此书的借阅次数
	            //System.out.println(pref);
	            k.set(userID);
	            v.set(itemID + ":" + pref);
		        context.write(k, v);
			}
	    }
	}
	
	public static class PartBookDataReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
		private final static Text v = new Text();
	    
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        StringBuilder sb = new StringBuilder();
	        Iterator<Text> datas = values.iterator();

	        while (datas.hasNext()) {
	        	sb.append("," + datas.next());                     
	        }
	        
	        v.set(sb.toString().replaceFirst(",", ""));
	        context.write(key, v);
	        System.out.println(key + "\t" + v);
	    }
	}
	
	//运行第一步的代码，按照用户的ID进行分组，计算所有图书出现的组合列表，得到用户对图书的借阅矩阵
	public static void run(Map<String, String> path) throws Exception {
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PartBookData");

        String input = path.get("PartBookDataInput");
        String output = path.get("PartBookDataOutput");

        HdfsDao hdfs = new HdfsDao(BookRecommendMain.HDFS, conf);
        hdfs.rmr(input);
        hdfs.mkdir(input);
        hdfs.copyFile(path.get("data"), input);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(PartBookDataMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setCombinerClass(PartBookDataReducer.class);
        
        job.setReducerClass(PartBookDataReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));;
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
	}

}
