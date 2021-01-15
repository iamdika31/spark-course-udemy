package com.spark_course.sparkML;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
public class HousePriceAnalysis {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("House Price").master("local[*]").getOrCreate();

		Dataset<Row> csvData = spark.read()
								.option("header",true)
								.option("inferSchema",true)
								.csv("src/main/resources/kc_house_data.csv");
//		csvData.printSchema();
//		csvData.show();
		csvData = csvData.withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living"))).withColumnRenamed("price", "label");
		Dataset<Row>[] dataSplits = csvData.randomSplit(new double[] {0.9,0.1});
		Dataset<Row> trainingAndTestData = dataSplits[0];
		Dataset<Row> holdOutData = dataSplits[1];

		
		
		StringIndexer conditionIndexer= new StringIndexer();
		conditionIndexer.setInputCol("condition");
		conditionIndexer.setOutputCol("conditionIndex");
//		csvData = conditionIndexer.fit(csvData).transform(csvData);
		
		StringIndexer gradeIndexer= new StringIndexer();
		gradeIndexer.setInputCol("grade");
		gradeIndexer.setOutputCol("gradeIndex");
//		csvData = gradeIndexer.fit(csvData).transform(csvData);
		
		StringIndexer zipIndexer= new StringIndexer();
		zipIndexer.setInputCol("zipcode");
		zipIndexer.setOutputCol("zipIndex");
//		csvData = zipIndexer.fit(csvData).transform(csvData);
		
		
		OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
		encoder.setInputCols(new String[] {"conditionIndex","gradeIndex","zipIndex"});
		encoder.setOutputCols(new String[] {"conditionVector","gradeVector","zipcodeVector"});
//		csvData = encoder.fit(csvData).transform(csvData);
		
		VectorAssembler vectorAssembler = new VectorAssembler()
											.setInputCols(new String[] {"bedrooms","bathrooms","sqft_living","sqft_above_percentage","floors","conditionVector","gradeVector","zipcodeVector","waterfront" })
											.setOutputCol("features");
//		Dataset<Row> csvDataWithFeature = vectorAssembler.transform(csvData);
		
		

		
		
		LinearRegression linearRegression = new LinearRegression();
		
		ParamGridBuilder paramGridBuilder = new ParamGridBuilder();
		
		ParamMap[] paramMap = paramGridBuilder.addGrid(linearRegression.regParam(), new double[] {0.01,0.1,0.5})
											  .addGrid(linearRegression.elasticNetParam(),new double[] {0,0.5,1})
											  .build();
		
		TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
														.setEstimator(linearRegression)
														.setEvaluator(new RegressionEvaluator().setMetricName("r2"))
														.setEstimatorParamMaps(paramMap)
														.setTrainRatio(0.8);

		Pipeline pipeline = new Pipeline();
		pipeline.setStages(new PipelineStage[] {conditionIndexer,gradeIndexer,zipIndexer,encoder,vectorAssembler,trainValidationSplit});
		PipelineModel pipelineModel = pipeline.fit(trainingAndTestData);		
		TrainValidationSplitModel model = (TrainValidationSplitModel) pipelineModel.stages()[5];
		LinearRegressionModel lrModel = (LinearRegressionModel	) model.bestModel();
		
		Dataset<Row> holdOutResult = pipelineModel.transform(holdOutData);
		holdOutResult = holdOutResult.drop("prediction");
		holdOutResult.show();
		
		System.out.println("the training data r2 value is " + lrModel.summary().r2() + " and the RMSE is "+ lrModel.summary().rootMeanSquaredError());		
		System.out.println("the test data r2 value is " + lrModel.evaluate(holdOutResult).r2() + " and the RMSE is "+ lrModel.evaluate(holdOutResult).rootMeanSquaredError());

		System.out.println("coefficient : "+ lrModel.coefficients() + " intercept : " + lrModel.intercept());
		System.out.println("reg param : " + lrModel.getRegParam() + " elastic net param : "+ lrModel.getElasticNetParam());

	}

}
