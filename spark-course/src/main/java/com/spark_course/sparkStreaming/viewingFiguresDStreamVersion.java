package com.spark_course.sparkStreaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;


public class viewingFiguresDStreamVersion {

	public static void main(String[] args) throws InterruptedException {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("viewingFigures");
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "group1");
		kafkaParams.put("auto.offset.reset","latest" );
		
		
		Collection<String> topics = Arrays.asList("viewrecords");
		
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(sc, LocationStrategies.PreferConsistent(),
									  ConsumerStrategies.Subscribe(topics, kafkaParams	));

		JavaPairDStream<Long,String> results = stream.mapToPair(item -> new Tuple2<>(item.value(),5L))
														 .reduceByKeyAndWindow((x,y) -> x+y,Durations.minutes(60),Durations.minutes(1))//parameter ke tiga adalah slide
//														 sliding digunakan untuk menampilkan data selama waktu yang telah ditentukan namun di background process berjalan setiap detik 
														 .mapToPair(item -> item.swap())
														 .transformToPair(rdd -> rdd.sortByKey(false));
//		JavaDStream<String> result = stream.map(item -> item.value());
		results.print(50);
		sc.start();
		sc.awaitTermination();
	}

}
