package com.RUSpark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
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
		
    df.createOrReplaceTempView("netflix");
    Dataset<Row> partitioned = spark.sql(
    "SELECT _c0 as movie_id, ROUND(AVG(_c2), 2) as average_rating"
    + " FROM netflix"
    + " GROUP BY _c0"
    + " ORDER BY _c0");
    partitioned.show((int)partitioned.count());

    spark.stop();
	}

}
