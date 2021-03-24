package com.telkom.indiee

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

import java.util.Properties
import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.io.Source


object dummyTest {
  def main(args: Array[String]): Unit = {

    @transient lazy val logger = Logger.getLogger(getClass.getName)

    logger.info("Testing the spark run on the system")

    val spark = SparkSession.builder()
      .config(setSparkConfigs())
      .enableHiveSupport()
      .getOrCreate()

    logger.info("Spark Session Created")

    logger.info("Checking some big reads ")

    val dummyDF = spark.sql("select * from mevin_consumer.mobile_postpaid_eligible_basee")
    logger.info("Count retrieved is: "+dummyDF.count())

    logger.info("Finished executing the test ")

    spark.close()

  }

  def setSparkConfigs(): SparkConf = {
    val customSparkConfig = new SparkConf

    val cprogs = new Properties
    cprogs.load(Source.fromFile("spark.conf").bufferedReader())

    // following line for scala 2.12.xx
    // cprogs.forEach((k,v) => customSparkConfig.set(k.toString, v.toString))

    // following line is for scala 2.11.xx
    import  scala.collection.JavaConverters
    cprogs.asScala.foreach(kv => customSparkConfig.set(kv._1.toString, kv._2.toString))

    customSparkConfig
  }

}
