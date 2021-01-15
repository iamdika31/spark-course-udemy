package com.spark_course.sparkML;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.List;


public class CourseRecommendation {

	public static void main(String[] args) {
		
	Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("VPP chapter views").master("local[*]").getOrCreate();

		Dataset<Row> csvData = spark.read()
								.option("header",true)
								.option("inferSchema",true)
								.csv("src/main/resources/VPPcourseViews.csv");
		csvData = csvData.withColumn("proportionWatched", col("proportionWatched").multiply(100));
//		csvData.groupBy("userId").pivot("courseId").sum("proportionWatched").show();
//		csvData.show();
		
//		Dataset<Row>[] trainingAndHoldoutData = csvData.randomSplit(new double[] {0.9,0.1});
//		Dataset<Row> trainingData = trainingData = trainingAndHoldoutData[0];
//		Dataset<Row> holdOutData  = trainingAndHoldoutData[1];
		
		ALS als = new ALS()
					  .setMaxIter(10)
					  .setRegParam(0.1)
					  .setUserCol("userId")
					  .setItemCol("courseId")
					  .setRatingCol("proportionWatched");		
		ALSModel model = als.fit(csvData);	
		model.setColdStartStrategy("drop");
		
//		Dataset<Row> predictions = model.transform(csvData);
//		predictions.show();
//		Dataset<Row> userRecs = model.recommendForAllUsers(5);
//		userRecs.show();
//		List<Row> userRecsList = userRecs.takeAsList(5);
//		
//		for(Row r : userRecsList) {
//			int userId = r.getAs(0);
//			String recs = r.getAs(1).toString();
//			System.out.println("User "+userId+ " we might want to recommend "+recs);
//			System.out.println("this user has already watched: ");
//			csvData.filter("userId = "+userId).show();
//		}
		
		Dataset<Row> testData = spark.read()
						.option("header",true)
						.option("inferSchema",true)
						.csv("src/main/resources/vppChapterViewsTest.csv");
		model.transform(testData).show();
		model.recommendForUserSubset(testData, 5).show();
		
	}

}
