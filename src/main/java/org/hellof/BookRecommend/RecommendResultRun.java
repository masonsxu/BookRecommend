package org.hellof.BookRecommend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

class Cooccurrence {
    private String itemID1;
    private String itemID2;
    private int num;

    public Cooccurrence(String itemID12, String itemID22, int num) {
        super();
        this.itemID1 = itemID12;
        this.itemID2 = itemID22;
        this.num = num;
    }

    public String getItemID1() {
        return itemID1;
    }

    public void setItemID1(String itemID1) {
        this.itemID1 = itemID1;
    }

    public String getItemID2() {
        return itemID2;
    }

    public void setItemID2(String itemID2) {
        this.itemID2 = itemID2;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

}

public class RecommendResultRun {
	
	public static class RecommendResult_PartialMultiplyMapper extends Mapper<Object, Text, Text, Text> {
        private final static Text k = new Text();
        private final static Text v = new Text();

        private final static Map<String, List> cooccurrenceMatrix = new HashMap<String, List>();

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = BookRecommendMain.DELIMITER.split(values.toString());

            String[] v1 = tokens[0].split(":");
            String[] v2 = tokens[1].split(":");

            if (v1.length > 1) {// cooccurrence
                String itemID1 = v1[0];
                String itemID2 = v1[1];
                int num = Integer.parseInt(tokens[1]);                

                List list = null;
                if (!cooccurrenceMatrix.containsKey(itemID1)) {
                    list = new ArrayList();
                } else {
                    list = cooccurrenceMatrix.get(itemID1);
                }
                list.add(new Cooccurrence(itemID1, itemID2, num));
                cooccurrenceMatrix.put(itemID1, list);
            }

            if (v2.length > 1) {// userVector
                String itemID = tokens[0];
                String userID = v2[0];
                int pref = Integer.parseInt(v2[1]);
                List<Cooccurrence> list = cooccurrenceMatrix.get(itemID);
                k.set(userID);
                for (Cooccurrence co : list) {
                    v.set(co.getItemID2() + "," + pref * co.getNum());
                    context.write(k, v);
                    System.out.println(k + "\t" + v);
                }

            }
        }
    }

    public static class RecommendResult_AggregateAndRecommendReducer extends Reducer<Object, Text, Text, Text> {
        private final static Text v = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> result = new HashMap<String, Double>();
            Iterator<Text> datas = values.iterator();
            
            while (datas.hasNext()) {
                String[] str = datas.next().toString().split(",");
                if (result.containsKey(str[0])) {
                    result.put(str[0], result.get(str[0]) + Double.parseDouble(str[1]));
                } else {
                    result.put(str[0], Double.parseDouble(str[1]));
                }
            }
            
            Iterator<String> iter = result.keySet().iterator();
            while (iter.hasNext()) {
                String itemID = (String) iter.next();
                double score = result.get(itemID);
                v.set(itemID + "," + score);
                context.write(key, v);
                System.out.println(key + "\t" + v);
            }
        }
    }
    
    public static void run(Map<String, String> path) throws Exception {
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RecommendResult");

        String input1 = path.get("RecommendResultInput1");
        String input2 = path.get("RecommendResultInput2");
        String output = path.get("RecommendResultOutput");

        HdfsDao hdfs = new HdfsDao(BookRecommendMain.HDFS, conf);
        hdfs.rmr(output);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(RecommendResult_PartialMultiplyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setCombinerClass(RecommendResult_AggregateAndRecommendReducer.class);
        
        job.setReducerClass(RecommendResult_AggregateAndRecommendReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}
