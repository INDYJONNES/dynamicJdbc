package com.telkom.indiee

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

import java.util.Properties
import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.io.Source

object dynamicJdbc extends Serializable {
  def main(args: Array[String]): Unit = {

    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Running the Dynamic JDBC Inserts")

    if(args.length < 4) {
      logger.error("Number of Arguments are invalid")
      logger.error(" Use : db_name table_name target_table write_mode")
      System.exit(1)
    }

    // getting the command line values
    val dbname = args(0);
    val tabname = args(1);
    val ttable = args(2);
    val wmode = args(3);
    logger.info("Local Object "+dbname+"."+tabname)

    logger.info("Creating the Spark Session for the run")
    val spark = SparkSession.builder()
      .config(setSparkConfigs())
      .enableHiveSupport()
      .getOrCreate()

    // Enable Following line is for debugging only
    logger.info("All Spark Configs used: " + spark.conf.toString())

    if( ! checkTableExists(spark,dbname,tabname)) {
        logger.error("Database OR TableName used DO NOT exist,  check the entries")
        System.exit(1)
    }

    val inputDF = loadDataFrame(spark,dbname,tabname)
    logger.info("Count : "+inputDF.count())
    // Writing out to JDBC destination
    inputDF.write.format("jdbc").option("url","jdbc:oracle:thin:@fpuncdb1.telkom.co.za:1527:unicadb").option("dbtable",ttable).option("user","campmart").option("password","campmart").option("driver","oracle.jdbc.driver.OracleDriver").mode(wmode).save()


    logger.info("Finished writing data into JDBC destination")
    // End of session Spark
    spark.stop()

  }

  def loadDataFrame(session: SparkSession, str: String, str1: String): DataFrame = {
    val tname = str+"."+str1
    val iDF = session.sql("select * from "+tname)
    return iDF
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

  def checkTableExists(session: SparkSession, dbn: String, tabn: String): Boolean = {
    // val use_db = s"use "+dbn
    session.catalog.tableExists(dbn,tabn)
    // session.sqlContext.sql(use_db)
    // session.sqlContext.tableNames.contains(tabn)
  }

}
