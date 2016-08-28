package com.mengls.study;

import org.apache.spark.sql.SparkSession;

/**
 * Created by mengls on 2016/8/21.
 */
public class Spark {

    public static SparkSession getSession() {
        SparkSession.Builder builder = SparkSession.builder()
                .master("local[8]")
                .appName("study-spark")
                .config("spark.sql.warehouse.dir", System.getProperty("user.dir") + "/spark-warehouse")//避免本地开发时sparkSession无法创建的问题
                .config("spark.driver.allowMultipleContexts", "true");

//        builder.config("spark.jars", "");
        return builder.getOrCreate();
    }
}
