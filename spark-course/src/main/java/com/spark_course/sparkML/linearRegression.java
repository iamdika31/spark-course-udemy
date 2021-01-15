package com.spark_course.sparkML;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class linearRegression {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("Gym Competition").master("local[*]").getOrCreate();

		Dataset<Row> csvData = spark.read()
								.option("header",true)
								.option("inferSchema",true)
								.csv("src/main/resources/GymCompetition.csv");
		
		StringIndexer genderIndex= new StringIndexer();
		genderIndex.setInputCol("Gender");
		genderIndex.setOutputCol("GenderIndex");
		csvData = genderIndex.fit(csvData).transform(csvData);
//		csvData.show();
		
		OneHotEncoderEstimator genderEncoder = new OneHotEncoderEstimator();
		genderEncoder.setInputCols(new String[] {"GenderIndex"});
		genderEncoder.setOutputCols(new String[] {"GenderVector"});
		csvData = genderEncoder.fit(csvData).transform(csvData);
//		csvData.show();
		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] {"Age","Height","Weight","GenderVector"});
		vectorAssembler.setOutputCol("features");
		Dataset<Row> csvDataWithFeature = vectorAssembler.transform(csvData);
		
		Dataset<Row> modelInputData = csvDataWithFeature.select("NoOfReps","features").withColumnRenamed("NoOfReps", "label");
		modelInputData.show();
		
		LinearRegression linearRegresion = new LinearRegression();
		LinearRegressionModel LRModel=  linearRegresion.fit(modelInputData);
		System.out.println("The model has intercept "+LRModel.intercept() + " and coefficients "+LRModel.coefficients());
		
		LRModel.transform(modelInputData).show();
	}

}
