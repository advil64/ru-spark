package com.RUSpark;

//tutorial: https://www.hellocodeclub.com/apache-spark-java-tutorial-simplest-guide-to-get-started/

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;


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

        // load the dataset into a spark rdd
        JavaRDD<String> photosData = sparkContext.textFile(InputPath);

        }

}
