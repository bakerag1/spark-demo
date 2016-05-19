package com.otherthanbacon.demo.spark;

/**
 * Copyright 2016 A. Baker
 */

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;


/**
 *
 * @author bakerag
 * @since 1.0
 *
 */
public class DataBridge {
  Logger logger = Logger.getLogger(DataBridge.class);

  public void tryIt() {
    logger.debug("configuring");
    SparkConf conf =
        new SparkConf().setAppName("DataBridge")
            .set("spark.cassandra.connection.host", "localhost").setMaster("local[4]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    logger.debug("reading");
    CassandraTableScanJavaRDD<String> cassandraRdd =
        CassandraJavaUtil.javaFunctions(sc)
            .cassandraTable("hotel", "hotels", CassandraJavaUtil.mapColumnTo(String.class))
            .select("name");
    logger.debug(cassandraRdd.collect());
  }
}
