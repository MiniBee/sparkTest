package com.phy.structuredStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * Created by Administrator on 2018/3/13.
 */
public class QuickStart {

    private static void structuredStreamingWorldCount(){
        String output = "D:\\Tools\\spark-2.3.0-bin-hadoop2.7\\examples\\src\\main\\resources";
        SparkSession spark = SparkSession.builder().appName("Java Structured Network Word Count").master("local").getOrCreate();
        Dataset<Row> lines = spark.readStream().format("socket").option("host", "39.106.188.182").option("port", 8088).load();
        Dataset<String> words = lines.as(Encoders.STRING()).flatMap(x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
        Dataset<Row> wordCounts = words.groupBy("value").count();
        StreamingQuery query = wordCounts.writeStream().outputMode("append").format("console").start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
        spark.conf();
    }

    private static void readFileAsStreaming(){
        String path = "D:\\Tools\\spark-2.3.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt";
        SparkSession spark = SparkSession.builder().appName("File as Streaming").master("local").getOrCreate();
        Dataset<Row> lines = spark.readStream().option("sep", ",")
                .schema(new StructType().add("name", "string").add("age", "integer"))
                .csv(path);
//        Dataset<String> words = lines.as(Encoders.STRING()).flatMap(x -> Arrays.asList(x.split(",")).iterator(), Encoders.STRING());
        Dataset<Row> wordCounts = lines.select("name", "age");
        StreamingQuery query = wordCounts.writeStream().outputMode("append").format("console").start();
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
        spark.close();
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        readFileAsStreaming();
//        structuredStreamingWorldCount();
    }

}
