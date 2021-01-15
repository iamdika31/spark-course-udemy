package com.spark_course.spark_sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class ExamResult {
	
	// OLD FASHIONED SYNTAX UDF
	private static UDF2<String, String, Boolean> hasPassedFunction = new UDF2<String, String, Boolean>() {
		public Boolean call(String grade, String subject) throws Exception{
			if(subject.equals("Biology")) {
				if(grade.startsWith("A")) return true;
				return false;
			}
			return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
	 
		}
	};

	
	
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("test spark sql").master("local[*]").getOrCreate();
		Dataset<Row> ds = spark.read()
						 .option("header",true)
						 .option("inferSchema",true)
						 .csv("src/main/resources/exams/students.csv");
//		Dataset<Row> simpleAgg = ds.groupBy("subject").agg(max(col("score")).alias("max score"),
//									   min(col("score")).alias("min score"));
		
//=======================================Building Pivot table with multiple Aggregations ====================================
//		simpleAgg.show();
//		Dataset<Row> practicePivot = ds.groupBy("subject").pivot("year").agg(round(avg(col("score")),2).alias("avg_score"),
//																	   round(stddev(col("score")),2).alias("stddev_score"));
//		practicePivot.show();
		
//========================================= User Defined Function =============================================================		
		spark.udf().register("hasPassed", (String grade,String subject)-> {
			if(subject.equals("Biology")) {
				if(grade.startsWith("A")) return true;
				return false;
			}
			return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
		},DataTypes.BooleanType);
		
		//using old fashioned UDF
//		spark.udf().register("hasPassed", (String grade,String subject)-> hasPassedFunction.call(grade, subject),DataTypes.BooleanType);

		
		ds = ds.withColumn("pass", callUDF("hasPassed", col("grade"),col("subject")));
		ds.show();
	}

}
