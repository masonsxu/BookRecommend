package org.hellof.BookRecommend;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.BasicConfigurator;

public class BookRecommendMain {
	public static final String HDFS = "hdfs://localhost:8020";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();// 自动快速的使用缺省log4j环境
		Map<String, String> path = new HashMap<String, String>();
        path.put("data", "CountUserBook.csv");
        path.put("PartBookDataInput", HDFS + "/hellof/hdfs/BookRecommend");
        path.put("PartBookDataOutput", path.get("PartBookDataInput") + "/PartBookData");
        path.put("UserVectorToConoccurrenceInput", path.get("PartBookDataOutput"));
        path.put("UserVectorToConoccurrenceOutput", path.get("PartBookDataInput") + "/UserVectorToConoccurrence");
        path.put("MergeBorrowAppearInput1", path.get("PartBookDataOutput"));
        path.put("MergeBorrowAppearOutput1", path.get("PartBookDataInput") + "/MergeBorrowAppear_1");
        path.put("MergeBorrowAppearInput2", path.get("UserVectorToConoccurrenceOutput"));
        path.put("MergeBorrowAppearOutput2", path.get("PartBookDataInput") + "/MergeBorrowAppear_2");
        path.put("RecommendResultInput1", path.get("MergeBorrowAppearOutput1"));
        path.put("RecommendResultInput2", path.get("MergeBorrowAppearOutput2"));
        path.put("RecommendResultOutput", path.get("PartBookDataInput") + "/RecommendResult");
        
        PartBookDataRun.run(path);
        UserVectorToConoccurrenceRun.run(path);
        MergeBorrowAppearRun.run1(path);
        MergeBorrowAppearRun.run2(path);
        RecommendResultRun.run(path);
        
		System.exit(0);
	}

}
