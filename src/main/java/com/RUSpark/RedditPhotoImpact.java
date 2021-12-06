package com.RUSpark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }

    // dataset path
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

    // Impactid = sum( upvotes + downvotes + comments) for each image across subreddits
    Map<String, String> aggregates = new HashMap<String, String>() {{
      put("_c4", "sum");
      put("_c5", "sum");
      put("_c6", "sum");
    }};
    
    Dataset<Row> groupedDf = df.groupBy("_c0").agg(aggregates);
    Dataset<Row> resultDf = groupedDf.withColumn("total_impact", groupedDf.col("sum(_c4)").plus(groupedDf.col("sum(_c5)")).plus(groupedDf.col("sum(_c6)"))).withColumnRenamed("_c0", "image_id").drop(groupedDf.col("sum(_c4)")).drop(groupedDf.col("sum(_c5)")).drop(groupedDf.col("sum(_c6)"));
    resultDf.show((int)resultDf.count());

    spark.stop();
  }
}
