package com.phy.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Administrator on 2018/3/2.
 */
public class PairRDDTest {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        SparkConf conf = new SparkConf().setAppName("Pair RDD test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String logFile = "E:\\lsh\\vrm\\180101To180117.txt";
        JavaRDD<String> lines = sc.textFile(logFile);
        JavaPairRDD<String, Integer> pairRDD = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                String[] words = s.split(",");
                List<Tuple2<String, Integer>> result = null;
                for (String word: words){
                    result.add(new Tuple2<>(word, 1));
                }
                return result.iterator();
            }
        });
        
        sc.close();

    }

}
