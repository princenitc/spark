package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
	public static void main(String[] args) {
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: FIRST WARNING");
		inputData.add("FATAL: FIRST FATAL");
		inputData.add("WARN: SECOND WARNING");
		inputData.add("ERROR: FIRST ERROR");
		inputData.add("WARN: THIRD WARNING");
		inputData.add("FATAL: SECOND FATAL");

		SparkConf conf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		sc.parallelize(inputData).mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
				.reduceByKey(Long::sum)
				.collect().forEach(s -> LOGGER.info(s + ""));
//		sums.collect().forEach(System.out::println);
		sc.close();
	}
}
