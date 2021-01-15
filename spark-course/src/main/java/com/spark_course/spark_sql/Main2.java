package com.spark_course.spark_sql;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main2 {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("test spark sql").master("local[*]").getOrCreate();

//		spark.conf().set("spark.sql.shuffle.partitions", 12);
		
		//IN MEMORY
//		List<Row> inMemory = new ArrayList<Row>();
//		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
//		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
//		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
//		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
//		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
//		
//		StructField[] fields = new StructField[] {
//				new StructField("level",DataTypes.StringType,false,Metadata.empty()),
//				new StructField("datetime",DataTypes.StringType,false,Metadata.empty())
//		};
//		StructType schema = new StructType(fields);
//		Dataset<Row> ds = spark.createDataFrame(inMemory,schema);

//=============================MULTIPLE GROUPINGS AND ORDERING ==============================================		
		Dataset<Row> ds = spark.read().option("header",true).csv("src/main/resources/biglog.txt");
		
		SimpleDateFormat input = new SimpleDateFormat("MMMM");
		SimpleDateFormat output = new SimpleDateFormat("M");
		spark.udf().register("monthNum",(String month) -> {
			Date inputDate = input.parse(month);
			return Integer.parseInt(output.format(inputDate));
		},DataTypes.IntegerType);
		
		ds.createOrReplaceTempView("logging_table");
		
		//USING UDF
//		Dataset<Row> rows = spark.sql("select level , date_format(datetime,'MMMM') as Month,count(1) as total_count  from logging_table group by level,Month order by monthNum(Month),level");
		
		//USING CAST
		Dataset<Row> rows = spark.sql("select level , date_format(datetime,'MMMM') as Month,count(1) as total_count,first(cast(date_format(datetime,'M') as int)) as monthnum  from logging_table group by level,Month order by monthnum,level");
		
//		rows.show(100);
//===================================USING DATAFRAME API ========================================================		
//		Dataset<Row> rows = ds.select(col("level"),date_format(col("datetime"), "MMMM")
//							  .alias("Month"),
//							  date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType))
//				     		  .groupBy(col("level"),col("Month"),col("monthnum")) 
//				     		  .count()
//				     		  .orderBy(col("monthnum"),col("level"))
//				     		  .drop(col("monthnum"))
//				     		  ;
		rows.show(100);
		rows.explain();
//===================================CREATE PIVOT TABLE ========================================================
//		Dataset<Row> rows = ds.select(col("level"),date_format(col("datetime"), "MMMM")
//		  .alias("Month"),
//		  date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
//		
//		Object[]months = new Object[] {"January","February","March","April","May","June","July","August","September","October","November","December"};
//		List<Object> columns = Arrays.asList(months);
//		rows = rows.groupBy("level").pivot("month",columns).count().na().fill(0);
//		rows.show();
	}

}
