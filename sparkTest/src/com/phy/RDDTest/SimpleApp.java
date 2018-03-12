package com.phy.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Array;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * Created by Administrator on 2018/3/2.
 */
public class SimpleApp {

    private static void test(){
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        String logFile = "E:\\lsh\\vrm\\180101To180117.txt";
        SparkConf conf = new SparkConf().setAppName("WordCount").
                setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> logData = sc.textFile(logFile);
        JavaRDD<String> lineLength =
                logData.flatMap(new FlatMapFunction<String, String>() {

                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(",")).iterator();
                    }
                });

        JavaPairRDD<String, Integer> pairs = lineLength.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> wordcount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        for (Tuple2<String, Integer> pair : wordcount.collect()) {
            System.out.println(pair._1 + ": " + pair._2);
        }

        sc.close();
    }

    private static void parallelizeTest(){
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        SparkConf conf = new SparkConf().setAppName("ParallelizeTest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd = sc.parallelize(data);
        int i = rdd.reduce((a, b) -> a+b);
        System.out.println(i);
        sc.close();
    }

    private static void externalDatasetTest(){
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        SparkConf conf = new SparkConf().setAppName("externalDataSet").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileData = sc.textFile("E:\\phy\\trophy\\streaming\\spark.txt");
        JavaRDD<String> words = fileData.flatMap( a -> Arrays.asList(a.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(a -> new Tuple2<String, Integer>(a, 1));
        JavaPairRDD<String, Integer> wordcounts = pairs.reduceByKey((a, b)-> a+b);
        List<Tuple2<String, Integer>> r = wordcounts.collect();
        for (Tuple2<String, Integer> i : r){
            System.out.println("===="+i._1+": "+i._2);
        }
        sc.close();
    }

    static class GetLength implements Function<String, Integer> {

        @Override
        public Integer call(String s) throws Exception {
            return s.split(" ").length;
        }
    }

    static class Sum implements Function2<Integer, Integer, Integer>{

        @Override
        public Integer call(Integer integer, Integer integer2) throws Exception {
            return integer + integer2;
        }
    }

    private static void passingFunctionTest(){
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        SparkConf conf = new SparkConf().setAppName("passingFunctionTest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileData = sc.textFile("E:\\phy\\trophy\\streaming\\spark.txt");
        JavaRDD<Integer> lineLength = fileData.map(new GetLength());
        int totalCount = lineLength.reduce(new Sum());
        System.out.println(totalCount);
        sc.close();
    }

    private static void unionTest(){
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        SparkConf conf = new SparkConf().setAppName("unionTest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> l1 = Arrays.asList(1,2,3,4,5);
        List<Integer> l2 = Arrays.asList(6,7,8,9,0);
        JavaRDD<Integer> i1 = sc.parallelize(l1);
        JavaRDD<Integer> i2 = sc.parallelize(l2);
        JavaRDD<Integer> i3 = i1.union(i2);
        List<Integer> i = i3.collect();
        for (int a : i){
            System.out.println("===>"+a);
        }
        sc.close();
    }

    private static void joinTest(){
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        SparkConf conf = new SparkConf().setAppName("joinTest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> l1 =Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 1));
        List<Tuple2<String, Integer>> l2 =Arrays.asList(new Tuple2<>("a", 2), new Tuple2<>("b", 2));
        JavaPairRDD<String, Integer> j1 = sc.parallelizePairs(l1);
        JavaPairRDD<String, Integer> j2 = sc.parallelizePairs(l2);
        List<Tuple2<String, Tuple2<Integer, Integer>>> r = j1.join(j2).collect();
        for (Tuple2<String, Tuple2<Integer, Integer>> a: r){
            System.out.println("====>" + a._1() + a._2());
        }
        sc.close();
    }

    public static void main(String[] args) {
        joinTest();
//        unionTest();
//        passingFunctionTest();
//        externalDatasetTest();
//        test();
//        parallelizeTest();

    }

}
