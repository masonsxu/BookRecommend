package org.hellof.BookRecommend;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hellof.BookRecommend.HdfsDao.HdfsDao;

public class MergeBorrowAppearRun {
	
	public static class MergeBorrowAppear1_Mapper extends Mapper<Object, Text, Text, Text> {
        private final static Text k = new Text();
        private final static Text v = new Text();

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = BookRecommendMain.DELIMITER.split(values.toString());
            
            for (int i = 1; i < tokens.length; i++) {
                String[] vector = tokens[i].split(":");
                String itemID = vector[0];
                String pref = vector[1];

                k.set(itemID);
                v.set(tokens[0] + ":" + pref);
                context.write(k, v);
                System.out.println(k + "\t" + v);
            }
        }
    }
	
	public static void run1(Map<String, String> path) throws Exception {
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MergeBorrowAppear");

        String input = path.get("MergeBorrowAppearInput1");
        String output = path.get("MergeBorrowAppearOutput1");

        HdfsDao hdfs = new HdfsDao(BookRecommendMain.HDFS, conf);
        hdfs.rmr(output);

        job.setJarByClass(MergeBorrowAppearRun.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MergeBorrowAppear1_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
	
	public static class MergeBorrowAppear2_Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text k = new Text();
        private final static IntWritable v = new IntWritable();

        /*@Override*/
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = BookRecommendMain.DELIMITER.split(values.toString());
            
            k.set(tokens[0]);
            v.set(Integer.parseInt(tokens[1]));
            context.write(k, v);
            System.out.println(k + "\t" + v);
        }
    }
	
	public static void run2(Map<String, String> path) throws Exception {
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MergeBorrowAppear");

        String input = path.get("MergeBorrowAppearInput2");
        String output = path.get("MergeBorrowAppearOutput2");

        HdfsDao hdfs = new HdfsDao(BookRecommendMain.HDFS, conf);
        hdfs.rmr(output);

        job.setJarByClass(MergeBorrowAppearRun.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MergeBorrowAppear2_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
