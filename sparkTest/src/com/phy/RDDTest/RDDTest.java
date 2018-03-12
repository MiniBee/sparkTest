package com.phy.test;

import com.phy.util.Print;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * Created by Administrator on 2018/3/2.
 */
public class RDDTest {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        SparkConf conf = new SparkConf().setAppName("RDDTest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String logFile = "E:\\lsh\\vrm\\180101To180117.txt";
        JavaRDD<String> lines = sc.textFile(logFile);
        JavaRDD<Integer> lineLength = lines.map(s -> s.split(",").length);
//        lineLength.persist(StorageLevel.MEMORY_ONLY());

        int totalWorld = lineLength.reduce((a, b) -> a+b);
        Print.print("total world: "+totalWorld);

        int totalWorld2 = lineLength.reduce((a, b) -> a+b);
        Print.print("total world: " + totalWorld2);
        sc.close();
    }

}
