package com.phy.DataFrame;

import org.apache.spark.sql.SparkSession;

/**
 * Created by Administrator on 2018/3/12.
 */
public class DataSetStart {

    private static void createSparkSession(){
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        SparkSession spark = SparkSession.builder().appName("Java spark sql").master("local").getOrCreate();
        spark.close();
    }
    public static void main(String[] args) {
        createSparkSession();
    }
}
