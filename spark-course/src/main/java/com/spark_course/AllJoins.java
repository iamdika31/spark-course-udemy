package com.spark_course;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class AllJoins {
	public static void main(String[] args) {
	   	Logger.getLogger("org.apache").setLevel(Level.WARN);
	    
	    SparkConf conf = new SparkConf().setAppName("starting spark").setMaster("local[*]");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
//=========================================JOINS =====================================================	    
	    List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
	    visitsRaw.add(new Tuple2<Integer, Integer>(4, 18));
	    visitsRaw.add(new Tuple2<Integer, Integer>(6, 4));
	    visitsRaw.add(new Tuple2<Integer, Integer>(10, 9));

	    List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
	    usersRaw.add(new Tuple2<Integer, String>(1, "John"));
	    usersRaw.add(new Tuple2<Integer, String>(2, "Bob"));
	    usersRaw.add(new Tuple2<Integer, String>(3, "Alan"));
	    usersRaw.add(new Tuple2<Integer, String>(4, "Doris"));
	    usersRaw.add(new Tuple2<Integer, String>(5, "Marybelle"));
	    usersRaw.add(new Tuple2<Integer, String>(6, "Raquel"));
	    
	    JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
	    JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);
	    
	    //INNER JOIN
	    JavaPairRDD<Integer,Tuple2<Integer, String>> joinedRDD = visits.join(users);
//	    joinedRDD.collect().forEach(System.out::println);
	    
	    
	    //LEFT OUTER JOIN
	    JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> LeftOuter = visits.leftOuterJoin(users);
//	    LeftOuter.collect().forEach(it -> System.out.println(it._2._2.orElse("BLANK").toUpperCase()));
	    
	    //RIGHT OUTER JOIN
	    JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> RightOuter = visits.rightOuterJoin(users);
//	    RightOuter.collect().forEach(it -> System.out.println(" User "+it._2._2 + " had "+it._2._1.orElse(0)));
	    
	    //CARTESIANS
	    JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesians = visits.cartesian(users);
	    cartesians.collect().forEach(System.out::println);
	    sc.close();
	}
}
