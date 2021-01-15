package com.spark_course;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRDD {

	public static void main(String[] args) {
    	Logger.getLogger("org.apache").setLevel(Level.WARN);
    	
    	List<String> inputData = new ArrayList<>();
    	inputData.add("WARN: Tuesday 4 September 0405");
    	inputData.add("ERROR: Tuesday 4 September 0408");
    	inputData.add("FATAL: Wednesday 5 September 1632");
    	inputData.add("ERROR: Friday 7 September 1854");
    	inputData.add("WARN: Saturday 8 September 1942");
    	
    	
        SparkConf conf = new SparkConf().setAppName("starting spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
//        JavaRDD<String> originalLogMessage = sc.parallelize(inputData);
//        
//        JavaPairRDD<String,Long> pairRDD = originalLogMessage.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L ));
//        JavaPairRDD<String,Long> sumRDD = pairRDD.reduceByKey((value1,value2) -> value1 + value2); 
//        sumRDD.collect().forEach(System.out::println);
        
        
        //UR USE AS A SINGLE LINE
//        sc.parallelize(inputData)
//          .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L ))
//          .reduceByKey((value1,value2) -> value1 + value2)
//          .collect().forEach(System.out::println);
        
        
//=====================================FLATMAPS AND FILTERS=================================================        
        JavaRDD<String> sentences = sc.parallelize(inputData);
        JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
        
        //example of filters
        JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);
        filteredWords.collect().forEach(System.out::println);
	}

}
