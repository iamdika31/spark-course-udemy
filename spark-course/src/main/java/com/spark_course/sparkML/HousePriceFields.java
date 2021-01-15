package com.spark_course.sparkML;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceFields {
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("House Price").master("local[*]").getOrCreate();

		Dataset<Row> csvData = spark.read()
								.option("header",true)
								.option("inferSchema",true)
								.csv("src/main/resources/kc_house_data.csv");
//		csvData.describe().show();
		
		csvData = csvData.drop("id","date","waterfront","view","condition","grade","yr_renovated","zipcode","lat","long");

		for(String col:csvData.columns()) {
			System.out.println("The correlation beetween price and " +col + " is "+ csvData.stat().corr("price", col));			
		}
		
		csvData = csvData.drop("sqft_lot","yr_built","sqft_living15","sqft_lot5");
		for(String col1: csvData.columns()) {
			for(String col2: csvData.columns()) {
				System.out.println("The correlation beetween "+ col1 +" and " +col2 + " is "+ csvData.stat().corr(col1, col2));							
			}
		}
	}
}
