package com.RUSpark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RedditHourImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		// start a spark session
    SparkSession spark = SparkSession
      .builder()
      .appName("redditPhotoImpact")
      .getOrCreate();

    // load the dataset into a dataframe
    Map<String, String> options = new HashMap<String, String>() {{
      put("inferSchema", "true");
      put("delimiter", ",");
    }};

    Dataset<Row> df = spark.read().options(options).csv(InputPath);
    
    df.createOrReplaceTempView("reddit");
    Dataset<Row> partitioned = spark.sql(
    "SELECT HOUR(FROM_UNIXTIME(_c1)) as hour, (SUM(_c4) + SUM(_c5) + SUM(_c6)) as hour_impact"
    + " FROM reddit"
    + " GROUP BY HOUR(FROM_UNIXTIME(_c1))"
    + " ORDER BY hour");
    partitioned.show((int)partitioned.count());

    spark.stop();
	}
}