package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.output.BrokenWriter;
import org.apache.hadoop.mapreduce.jobhistory.TaskUpdatedEvent;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.Optional;
import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures 
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = true;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		JavaPairRDD<Integer,Integer> courseChapterTotal = chapterData.mapToPair(tuple -> new Tuple2<Integer, Integer>(tuple._2, 1)).reduceByKey(Integer::sum);
		JavaPairRDD<Integer,Integer> distinctViewData = viewData.distinct().mapToPair(tuple -> new Tuple2<>(tuple._2,tuple._1));
		// Creating a joined view of the chapter, user and chapter course
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedChapterAndViews = distinctViewData.join(chapterData);
		// Since each RDD represents a chapter we can drop the chapterID ((userdID, Chapter), views)
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> droppedChapterRDD = joinedChapterAndViews.mapToPair(row -> new Tuple2<>(row._2, 1)).reduceByKey(Integer::sum);
		// Now userID is not relevant so dropping userID (chapterId, views )
		JavaPairRDD<Integer, Integer> chatperViewedData = droppedChapterRDD.mapToPair(row -> new Tuple2<>(row._1._2, row._2));
		// We have chapter -> views for different users now we need to add the total count as per chapters.
		JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> chapterViewsCount = chatperViewedData.leftOuterJoin(courseChapterTotal);
		// Now we have got RDD with Chapter course data
		// Let's create the RDD only with the percentage of views of total
		JavaPairRDD<Integer, Double> chapterPercentage = chapterViewsCount.mapToPair(row -> new Tuple2<>(row._1, (row._2._1.doubleValue() / row._2._2.get())));
		// Now adding the points as mentioned > 90% -> 10 points , 50-90 -> 4 Points , 25 - 50 -> 2 points, 0-25 -> 0 Points.
		JavaPairRDD<Integer, Integer> finalRdd = chapterPercentage.mapToPair(row -> new Tuple2<>(row._1, finalPoint(row._2))).reduceByKey(Integer::sum);
		// Now joining the tile
		JavaPairRDD<Integer, Tuple2<Integer, String>> Rdd = finalRdd.join(titlesData).mapToPair(row -> new Tuple2<>(row._2._1, new Tuple2<>(row._1, row._2._2))).sortByKey(false);
		Rdd.collect().forEach(System.out::println);
		sc.close();
	}


	private static Integer finalPoint(double num) {
		if(num >= .9)
			return 10;
		else if ((num >= .5)) {
			return 4;
		} else if ((num >= .25)) {
			return 2;
		}
		return 0;
	}
	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}
