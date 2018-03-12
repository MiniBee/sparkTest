package com.phy.DataFrame;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.glassfish.jersey.internal.util.collection.StringIgnoreCaseKeyComparator;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Created by Administrator on 2018/3/12.
 */
public class DataSetStart {

    private static void createSparkSession(){
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        SparkSession spark = SparkSession.builder().appName("Java spark sql").master("local").getOrCreate();
        spark.close();
    }

    private static void readJson(){
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        SparkSession spark = SparkSession.builder().appName("java spark sql").master("local").getOrCreate();
        Dataset<Row> df = spark.read()
                .json("D:\\Tools\\spark-2.3.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json");
        df.show();
        df.printSchema();
        df.select("name").show();
        df.select(col("name"), col("age").plus(1)).show();
        df.select(col("age").gt(21)).show();
        df.groupBy("age").count().show();
        spark.close();
    }

    private static void tempView(){
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        SparkSession spark = SparkSession.builder().appName("temp View").master("local").getOrCreate();
        Dataset<Row> df = spark.read().json("D:\\Tools\\spark-2.3.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json");
        df.createOrReplaceTempView("people");
        Dataset<Row> peopleDf = spark.sql("select * from people where age is not null");
        peopleDf.show();
        spark.close();
    }

    private static void createDataSet(){
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        SparkSession spark = SparkSession.builder().appName("dataset").master("local").getOrCreate();
        String path = "D:\\Tools\\spark-2.3.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json";
        Dataset<Persion> persionDS = spark.read().json(path).as(Encoders.bean(Persion.class));
        persionDS.show();
        spark.close();
    }

    private static void rddDataset(String path){
        SparkSession spark = SparkSession.builder().appName("dataset").master("local").getOrCreate();
        JavaRDD<Persion> persionRdd = spark.read().textFile(path).javaRDD().map(line -> {
            String[] parts = line.split(",");
            Persion persion = new Persion();
            persion.setAge(Integer.parseInt(parts[1].trim()));
            persion.setName(parts[0]);
            return persion;
        });
        Dataset<Row> personDF = spark.createDataFrame(persionRdd, Persion.class);
        personDF.createOrReplaceTempView("people");
        Dataset<Row> teenagerDF = spark.sql("select name from people where age between 13 and 19");
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagerDF.map(row -> "Name: " + row.getString(0), stringEncoder);
        teenagerNamesByIndexDF.show();
    }

    private static void rddDatasetWithoutClass(String path){
        SparkSession spark = SparkSession.builder().appName("dataset").master("local").getOrCreate();
        JavaRDD<String> personRDD = spark.sparkContext().textFile(path, 1).toJavaRDD();
        String schemaString = "name,age";
        List<StructField> fields = new ArrayList<>();
        for (String fieldName: schemaString.split(",")){
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schame = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRdd = personRDD.map(line -> {
           String[] attributes = line.split(",");
           return RowFactory.create(attributes[0], attributes[1].trim());
        });
        Dataset<Row> persionDF = spark.createDataFrame(rowRdd, schame);
        persionDF.show();
        spark.close();
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop-2.8.3");
        String path = "D:\\Tools\\spark-2.3.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt";
        rddDatasetWithoutClass(path);
//        rddDataset(path);
//        createDataSet();
//        tempView();
//        readJson();
//        createSparkSession();
    }
}
