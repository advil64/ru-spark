package com.RUSpark;

//tutorial: https://www.hellocodeclub.com/apache-spark-java-tutorial-simplest-guide-to-get-started/

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.*;
import java.util.HashMap;
import java.util.Map;


public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }

    // dataset path
    String InputPath = args[0];

    // logger and other configurations
    String logFile = "/common/home/ac1771/Desktop/ru-spark/log.md";
    SparkConf conf = new SparkConf().setAppName("redditPhotoImpact").setMaster("local[3]");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    // start a spark session
    SparkSession spark = SparkSession
      .builder()
      .appName("redditPhotoImpact")
      .config(conf)
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
    resultDf.show(5);
  }
}
