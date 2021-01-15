package com.spark_course.spark_sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
public class Main {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("test spark sql").master("local[*]").getOrCreate();
		Dataset<Row> ds = spark.read()
			 .option("header",true)
			 .csv("src/main/resources/exams/students.csv");
		
		//EXAMPLE FILTERS 
//		Dataset<Row> modernArtResult = ds.filter("subject = 'Modern Art' and year >= '2007' ");
		
		//EXAMPLE FILTERS USING LAMBDA'S
//		Dataset<Row> modernArtResult = ds.filter(row -> row.getAs("subject").equals("Modern Art"));
		
		//USING COLUMN NAME
//		Dataset<Row> modernArtResult = ds.filter(col("subject").equalTo("Modern Art")
//															   .and(col("year").geq(2007)));
		
		//USING SQL FUNC
		ds.createOrReplaceTempView("students");
		Dataset<Row> results = spark.sql("select * from students where subject = 'French'");
		results.show();
		spark.close();
	}

}
