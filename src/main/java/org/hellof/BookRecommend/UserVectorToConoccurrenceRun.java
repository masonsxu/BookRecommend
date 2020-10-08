package org.hellof.BookRecommend;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hellof.BookRecommend.HdfsDao.HdfsDao;

public class UserVectorToConoccurrenceRun {
	public static class UserVectorToCooccurrenceMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static Text k = new Text();
	    private final static IntWritable v = new IntWritable(1);

	    @Override
	    protected void map(Object key, Text values, Context context) throws IOException, InterruptedException {
	    	String[] tokens = BookRecommendMain.DELIMITER.split(values.toString()); 
	        
	        for (int i = 1; i < tokens.length; i++) {
	            String itemID = tokens[i].split(":")[0];
	            for (int j = 1; j < tokens.length; j++) {
	                String itemID2 = tokens[j].split(":")[0];
	                k.set(itemID + ":" + itemID2);
	                context.write(k, v);
	                System.out.println(key + "\t" + v);
	            }
	        }
	    }
	}
	
	
	public static class UserVectorToConoccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();

	    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    	Iterator<IntWritable> datas = values.iterator();
	    	
	    	int sum = 0;
	        while (datas.hasNext()) {
	            sum += ((IntWritable) datas.next()).get();
	        }
	        result.set(sum);
	        context.write(key, result);
	        System.out.println(key + "\t" + result);
	    }
	}

	
	public static void run(Map<String, String> path) throws Exception {
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "UserVectorToConoccurrence");

        String input = path.get("UserVectorToConoccurrenceInput");
        String output = path.get("UserVectorToConoccurrenceOutput");

        HdfsDao hdfs = new HdfsDao(BookRecommendMain.HDFS, conf);
        hdfs.rmr(output);
        
        job.setJarByClass(UserVectorToConoccurrenceRun.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(UserVectorToCooccurrenceMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setCombinerClass(UserVectorToConoccurrenceReducer.class);

        job.setReducerClass(UserVectorToConoccurrenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));      

        job.waitForCompletion(true);
    }
}
