package com.spark_course.sparkML;

import java.util.Arrays;
import java.util.List;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class vppFreeTrials {

	public static UDF1<String,String> countryGrouping = new UDF1<String,String>() {

		@Override
		public String call(String country) throws Exception {
			List<String> topCountries =  Arrays.asList(new String[] {"GB","US","IN","UNKNOWN"});
			List<String> europeanCountries =  Arrays.asList(new String[] {"BE","BG","CZ","DK","DE","EE","IE","EL","ES","FR","HR","IT","CY","LV","LT","LU","HU","MT","NL","AT","PL","PT","RO","SI","SK","FI","SE","CH","IS","NO","LI","EU"});
			
			if (topCountries.contains(country)) return country; 
			if (europeanCountries .contains(country)) return "EUROPE";
			else return "OTHER";
		}
		
	};
	
	public static void main(String[] args) {
	Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("VPP chapter views").master("local[*]").getOrCreate();

		Dataset<Row> csvData = spark.read()
								.option("header",true)
								.option("inferSchema",true)
								.csv("src/main/resources/vppFreeTrials.csv");
		
		spark.udf().register("countryGrouping",countryGrouping,DataTypes.StringType);
		csvData = csvData.withColumn("country", callUDF("countryGrouping", col("country")))
						 .withColumn("label", when(col("payments_made").geq(1),lit(1)).otherwise(lit(0)));
		
		StringIndexer countryIndexer = new StringIndexer();
		csvData = countryIndexer.setInputCol("country")
					 .setOutputCol("countryIndex")
					 .fit(csvData).transform(csvData);
		csvData.show();
		new IndexToString()
			.setInputCol("countryIndex")
			.setOutputCol("value")
			.transform(csvData.select("countryIndex").distinct())
			.show();
		
		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String [] {"countryIndex","rebill_period","chapter_access_count","seconds_watched"})
					   .setOutputCol("features");
		
		Dataset<Row> inputData = vectorAssembler.transform(csvData).select("label","features");
		inputData.show(); 
		
		Dataset<Row> []trainingAndHoldoutData = inputData.randomSplit(new double[] {0.8,0.2});
		Dataset<Row> trainingData = trainingAndHoldoutData[0];
		Dataset<Row> holdoutData = trainingAndHoldoutData[1];
		
//========================================= DECISION TREE CLASSIFIER ==============================================================
		DecisionTreeClassifier dtClassifier = new DecisionTreeClassifier();
		dtClassifier.setMaxDepth(4);
		DecisionTreeClassificationModel dtrModel = dtClassifier.fit(trainingData);
//		dtrModel.transform(holdoutData).show();
		System.out.println(dtrModel.toDebugString());
		
		Dataset<Row> predictions = dtrModel.transform(holdoutData);
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
		evaluator.setMetricName("accuracy");
		System.out.println("The accuracy Decision tree is "+ evaluator.evaluate(predictions));
		
//========================================= RANDOM FOREST CLASSIFIER ===============================================================
		RandomForestClassifier rfClassifier = new RandomForestClassifier();
		rfClassifier.setMaxDepth(4);
		RandomForestClassificationModel rfModel = rfClassifier.fit(trainingData);
		Dataset<Row> predictions2 = rfModel.transform(holdoutData);
		
		System.out.println(rfModel.toDebugString());
		System.out.println("The accuracy Random Forest is "+ evaluator.evaluate(predictions2));

	}

}
