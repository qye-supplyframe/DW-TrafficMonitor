package com.supplyframe.ds.hourlyAlert

import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{from_unixtime, _}
import com.supplyframe.dw.hdfsutilities.utils.SparkUtils

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object main {

  val oaHourlyDir = "/prod/etl/orthogonal_hourly"
  val trafficThreshold = 100
  val toEmail = List("qye@supplyframe.com")

  def saveFiles(df: DataFrame, fn: String, fileType: String): Unit = {
    fileType.toLowerCase match {
      case "avro" => df.coalesce(1).write.format("avro").mode("overwrite").save(fn)
      case "csv" => df.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"${fn}_csv")
      case "parquet" => df.coalesce(1).write.format("parquet").mode("overwrite").save(fn)
      case _ =>
        df.write.format("avro").mode("overwrite").save(fn)
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"${fn}_csv")
    }
  }

  def getHourTraffic(spark: SparkSession, runDate: String, runHour: String): Either[String, DataFrame] = {

    val datePath = runDate.replace("-", "/")
    val curHourDir = s"$oaHourlyDir/$datePath/$runHour"
    val headerPath = s"$curHourDir/_logs/*.tsv"
    val curHourPath = s"$curHourDir/p*"

    // Check if the current hour directory exists
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val pathExists = fs.exists(new Path(curHourDir))

    if (!pathExists) {
      return Left(s"Alert: Directory $curHourDir does not exist")
    }

    val headerSchema = spark.read.option("delimiter", "\t").option("header", "true").csv(headerPath).schema
    val currentHourDf = spark.read.option("delimiter", "\t").option("header", "false").schema(headerSchema).csv(curHourPath)

    val hourlyClicks = currentHourDf.withColumnRenamed("ct_flume_host", "server"
    ).withColumn("date_hour", date_format(from_unixtime(col("ct_datetime") / 1000), "yyyy-MM-dd HH:00")
    ).groupBy("server", "date_hour").agg(
      count("*").alias("traffic")
    )

    Right(hourlyClicks)

  }

  def sendAlertEmail(toEmail: List[String], subject: String, oaHourlyDir: String,
                     trafficThreshold: Int, errorMsg: Option[String] = None, dfString: Option[String] = None) = {

    val body = errorMsg match {
      case Some(error) =>
        s"""
           |Hi Team,
           |
           |This traffic alert is generated using orthogonal hourly data at $oaHourlyDir.
           |
           |Alert Criteria:
           |- Triggered when the directory is missing for the current or previous hour.
           |
           |Details:
           |$error
           |
           |Please investigate the issue.
           |
           |Best,
           |Data Platform Engineer Team
           |""".stripMargin

      case None =>
        s"""
           |Hi Team,
           |
           |This traffic alert is generated using orthogonal hourly data at $oaHourlyDir.
           |
           |Alert Criteria:
           |- Triggered when there is barely no traffic (<$trafficThreshold) this hour but there was traffic last hour.
           |
           |Below is a list of servers with significantly reduced traffic compared to the last hour:
           |
           |${dfString.getOrElse("")}
           |
           |Best,
           |Data Platform Engineer Team
           |""".stripMargin
    }

    println("---- sending alert email ----")
    SparkUtils.sendEmail(toEmail, subject, body)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("yarn")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.shuffle.blockTransferService", "nio")
    conf.set("spark.ui.showConsoleProgress", "true")
    conf.set("spark.shuffle.service.enabled", "true")
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.sql.avro.compression.codec", "snappy")
    conf.set("spark.speculation", "false")
    conf.set("spark.hadoop.mapreduce.map.speculative", "false")
    conf.set("spark.hadoop.mapreduce.reduce.speculative", "false")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // arguments
    val runDate = args(0)
    val runHour = args(1)
    val outputPath = args(2)

    println(s"---- $runDate, $runHour:00 ----")

    //    val runDate = "2024-02-14"
//    val runHour = "06"
//    val outputPath = "/user/qye/adhoc_detect_system_outage/QA_traffic_alert"

    // Combine runDate and runHour into a LocalDateTime
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00")
    val currentDateTime = LocalDateTime.parse(s"$runDate $runHour:00", formatter)

    // Subtract 1 hour
    val previousDateTime = currentDateTime.minusHours(1)

    // Extract the new runDate and runHour
    val previousRunDate = previousDateTime.toLocalDate.toString
    val previousRunHour = f"${previousDateTime.getHour}%02d"

    val curHourTrafficResult = getHourTraffic(spark, runDate, runHour)
    val preHourTrafficResult = getHourTraffic(spark, previousRunDate, previousRunHour)

    // Handle potential errors
    if (curHourTrafficResult.isLeft || preHourTrafficResult.isLeft) {
      val errorMsg = curHourTrafficResult.left.getOrElse(preHourTrafficResult.left.get)

      sendAlertEmail(
        toEmail = toEmail,
        subject = s"($runDate $runHour:00) Orthogonal Hourly Server Traffic Alert - Directory Missing",
        oaHourlyDir = oaHourlyDir,
        trafficThreshold = trafficThreshold,
        errorMsg = Some(errorMsg)
      )

      return
    }

    val curHourTraffic = curHourTrafficResult.right.get
    val preHourTraffic = preHourTrafficResult.right.get.drop("date_hour")
      .withColumnRenamed("traffic", "traffic_prev1h")

    val defaultDateHour = s"$runDate $runHour:00"

    val alertDf = preHourTraffic.join(curHourTraffic, Seq("server"), "outer"
    ).withColumn("date_hour", coalesce(col("date_hour"), lit(defaultDateHour))
    ).withColumn("traffic", coalesce(col("traffic"), lit(0))
    ).withColumn("traffic_prev1h", coalesce(col("traffic_prev1h"), lit(0))
    ).filter(col("traffic")<trafficThreshold
    ).filter(col("traffic_prev1h")>=trafficThreshold
    ).select("server", "date_hour", "traffic", "traffic_prev1h"
    ).orderBy(asc("traffic"), desc("traffic_prev1h")
    )

    val numRow = alertDf.count().toInt

    if (numRow>0) {
      // save df
      val runHourOutput = runDate.replace("-", "_") + "_" + runHour
      val alertPath = s"$outputPath/alert/$runHourOutput/server"
      saveFiles(alertDf, alertPath, "csv")

      // send email
      val dfString = SparkUtils.showString(alertDf, numRow, false)

      sendAlertEmail(
        toEmail = List("qye@supplyframe.com"),
        subject = s"($runDate $runHour:00) Orthogonal Hourly Server Traffic Alert - Minimum Traffic",
        oaHourlyDir = oaHourlyDir,
        trafficThreshold = trafficThreshold,
        dfString = Some(dfString)
      )

    } else {
      println("--- No alert ----")
    }

  }
}
