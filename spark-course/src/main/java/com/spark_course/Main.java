package com.spark_course;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Hello world!
 *
 */

class IntegerWithSquareRoot{
	private int originalNumber;
	private double squareRoot;
	public IntegerWithSquareRoot(int i ) {
		this.originalNumber = i;
		this.squareRoot = Math.sqrt(originalNumber);
		
	}
}
public class Main 
{
    public static void main( String[] args )
    {
    	Logger.getLogger("org.apache").setLevel(Level.WARN);
    	
    	List<Integer> inputData = new ArrayList<>();
    	inputData.add(35);
    	inputData.add(12);
    	inputData.add(90);
    	inputData.add(20);
    	
        System.out.println( "Hello World!" );
        SparkConf conf = new SparkConf().setAppName("starting spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
//        JavaRDD<Integer> myRDD = sc.parallelize(inputData);
//        Integer result = myRDD.reduce((value1,value2) -> value1 + value2 );
//        System.out.println(result);
        
//        JavaRDD<Double> sqrtRDD= myRDD.map(value1 -> Math.sqrt(value1));
//        sqrtRDD.foreach(value -> System.out.println(value));
        //or
//        sqrtRDD.collect().forEach(System.out::println);
        
        //how many elements in sqrtRDD
        //using just map and reduce
//        JavaRDD<Long> singleJavaRDD = sqrtRDD.map(value1 -> 1L);
//        Long countResult = singleJavaRDD.reduce((value1,value2) -> value1+value2);
//        System.out.println(countResult);
//        System.out.println(sqrtRDD);
        
//------------------------------ VIDEO BAGIAN 5: Tuples ---------------------------------------
        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
        IntegerWithSquareRoot iwsr = new IntegerWithSquareRoot(9);
        //FIRST EXAMPLE USING CLASS
//        JavaRDD<IntegerWithSquareRoot> sqrtRDD = originalIntegers.map(value ->  new IntegerWithSquareRoot(value));
        //OR USE TUPLE
        JavaRDD<Tuple2<Integer, Double>> sqrtRDD = originalIntegers.map(value ->  new Tuple2<>(value, Math.sqrt(value)));
        sqrtRDD.collect().forEach(System.out::println);
       sc.close();
    }
}
