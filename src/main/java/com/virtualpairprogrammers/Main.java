package com.virtualpairprogrammers;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
	public static void main(String[] args) {
		List<String> inputData = new ArrayList<>();
		inputData.add("brown fox jumped over" +
				              "  " +
				              "lazy fox");

		SparkConf conf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		sc.parallelize(inputData).flatMap(value -> Arrays.asList(value.split(" ")).iterator())
				.collect().forEach(s -> LOGGER.info(s + " "));
		sc.close();
	}
}
