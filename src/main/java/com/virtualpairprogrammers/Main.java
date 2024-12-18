package com.virtualpairprogrammers;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Main {
	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> initialRDD = sc.textFile("/Users/prince/Downloads/Practicals/Starting Workspace/Project/src/main/resources/subtitles/input.txt");


		JavaPairRDD<String,Long> prd =  initialRDD.map(sentence -> sentence
										 .replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
				                         .filter(sentence -> !sentence.trim().isEmpty())
				                         .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
				                         .filter(Util::isNotBoring)
				                         .mapToPair(word -> new Tuple2<>(word,1L))
										 .reduceByKey(Long::sum);
		JavaPairRDD<Long,String> jrdd = prd.mapToPair(tuple -> new Tuple2<>(tuple._2() , tuple._1())).sortByKey(false);
		jrdd.take(10).forEach(System.out::println);
//		jrdd.collect().forEach(System.out::println);
		sc.close();
	}
}
