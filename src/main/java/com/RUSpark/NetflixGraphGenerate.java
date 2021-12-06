package com.RUSpark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class NetflixGraphGenerate {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file>");
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

    Map<String, String> aggregates = new HashMap<String, String>() {{
      put("_c1", "collect_list");
    }};

    Dataset<Row> groupedDf = df.groupBy("_c0", "_c2").agg(aggregates);
    
    JavaRDD<List<Integer>> jrdd = groupedDf.javaRDD().map(r -> (List)r.getList(2));
    JavaPairRDD<String, Integer> ones = jrdd.flatMapToPair(s -> {
      
      ArrayList<Tuple2<String,Integer>> arr = new ArrayList<>();
      
      for(int i = 0; i < s.size(); i++){
        for(int j = i; j < s.size(); j++){
          if(i != j){
            
            if(s.get(i) < s.get(j)){
              arr.add(new Tuple2<>(s.get(i) + " " + s.get(j), 1));
            } else{
              arr.add(new Tuple2<>(s.get(i) + " " + s.get(j), 1));
            }
            
          }
        }
      }

      return arr.iterator();
    });

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + " " + tuple._2());
    }

    spark.stop();
		
	}

}
