package com.supplyframe.ds.dailyAlert

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.supplyframe.dw.hdfsutilities.utils.SparkUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import java.time.{LocalDate, YearMonth}
import java.time.format.DateTimeFormatter

object main {

  val ecommerceLayerDir = "/prod/etl/OrthogonalAggregations/EcommerceLayer"

  val minClicks = 50
  val minIPUA = 50
  val minDomain = 10
  val ratioThreshold = 0.01

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

  def getDateRange(start: String, end: String): Seq[String] = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val startDate = LocalDate.parse(start, formatter)
    val endDate = LocalDate.parse(end, formatter)
    val days = Iterator.iterate(startDate)(_.plusDays(1)).takeWhile(!_.isAfter(endDate)).map(_.toString).toSeq
    days
  }

  def getRawDataPrev21Days(spark: SparkSession, runDate: String): (DataFrame, DataFrame) = {

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val runDateObj = LocalDate.parse(runDate, formatter)
    val startDateObj = runDateObj.minusDays(21)
    val startDate = startDateObj.toString

    // get months
    val runMonthObj = YearMonth.from(runDateObj)
    val prevMonthObj = YearMonth.from(startDateObj)

    // Generate paths
    val runMonthPath = s"$ecommerceLayerDir/${runMonthObj.getYear}/${f"${runMonthObj.getMonthValue}%02d"}/p*"
    val prevMonthPath = s"$ecommerceLayerDir/${prevMonthObj.getYear}/${f"${prevMonthObj.getMonthValue}%02d"}/p*"

    // Check if prevMonth is the same as runMonth
    val paths = if (prevMonthObj == runMonthObj) {
      Seq(prevMonthPath)
    } else {
      Seq(prevMonthPath, runMonthPath)
    }
//    paths.foreach(println)

    // read data
    val df = spark.read.parquet(paths: _*).filter(col("date") <= runDate
    ).filter(col("country") =!= "***"
    ).filter(col("date") >= startDate
    ).filter(col("date") <= runDate
    )

    val allDates = getDateRange(startDate, runDate).map(date => Row(date))
    val schema = StructType(StructField("date", StringType, nullable = true) :: Nil)
    val allDatesDF = spark.createDataFrame(spark.sparkContext.parallelize(allDates), schema)

    (df, allDatesDF)

  }

  def getDailyClicks(df: DataFrame, granularity: Seq[String], runDate: String): DataFrame = {

    // compute daily clicks
    val dailyGranularity = granularity :+ "date"
    val dailyClicks = df.filter(col("clicks") === 1
    ).groupBy(dailyGranularity.head, dailyGranularity.tail: _*).agg(
      sum("clicks").alias("clicks"),
      countDistinct(col("master_user_group_id")).alias("count_master_user_group_id"),
      countDistinct(col("ip_address_user_agent")).alias("count_ip_address_user_agent"),
      countDistinct(col("city_code")).alias("count_city_code"),
      countDistinct(col("company_name")).alias("count_company_name"),
      countDistinct(col("domain_name")).alias("count_domain_name")
    )

    // estimate of clicks based on 21 days record
//    println("-------- get difference clicks --------")
    val windowSpecCountry = Window.partitionBy(granularity.head, granularity.tail: _*).orderBy("date")
    val dailyEstimates = dailyClicks.withColumn("prev_clicks_1days", lag("clicks", 1).over(windowSpecCountry)
    ).withColumn("prev_clicks_2days", lag("clicks", 2).over(windowSpecCountry)
    ).withColumn("prev_clicks_3days", lag("clicks", 3).over(windowSpecCountry)
    ).withColumn("prev_clicks_7days", lag("clicks", 7).over(windowSpecCountry)
    ).withColumn("prev_clicks_14days", lag("clicks", 14).over(windowSpecCountry)
    ).withColumn("prev_clicks_21days", lag("clicks", 21).over(windowSpecCountry)
    ).withColumn("avg_prev_3days",
      (col("prev_clicks_1days") + col("prev_clicks_2days") + col("prev_clicks_3days")) / 3.0
    ).withColumn("estimate",
      (col("prev_clicks_7days") + col("prev_clicks_14days") + col("prev_clicks_21days")) / 3.0
    ).withColumn("ratio",
      when(col("estimate") > 0, col("clicks") / col("estimate"))
    )

    // add rank
    val totalDailyTraffic = dailyEstimates.groupBy("date").agg(
      sum("clicks").alias("daily_total_clicks")
    )

    // join
//    println("-------- join difference clicks --------")
    val windowSpecRank = Window.partitionBy("date").orderBy(col("clicks").desc)

    dailyEstimates.join(totalDailyTraffic, Seq("date"), "left"
    ).withColumn("daily_traffic_percentage", col("clicks") * 1.0 / col("daily_total_clicks")
    ).withColumn("daily_cumulative_percentage",
      sum("daily_traffic_percentage").over(windowSpecRank)
    ).withColumn("daily_rank", rank().over(windowSpecRank)
    )

  }

  def alertTraffic(dailyRankFull: DataFrame, granularity: Seq[String], runDate: String): DataFrame = {

    val alertColumns = granularity ++ Seq("date_7_days_ahead", "alert_date")

    val alert = dailyRankFull.filter(col("estimate") > minClicks
    ).filter(col("ratio") < ratioThreshold
    ).filter(col("median_count_ip_address_user_agent") > minIPUA
    ).filter(col("median_count_domain_name") > minDomain
    ).filter(col("date") === runDate
    ).withColumnRenamed("date", "alert_date"
    ).withColumn("date_7_days_ahead", date_format(
      col("alert_date") - expr("INTERVAL 7 DAYS"), "yyyy-MM-dd")
    ).select(alertColumns.head, alertColumns.tail: _*
    )

    val selectedColumns = granularity :+ "date" :+ "clicks"

    val joinedWithDateRange = dailyRankFull.join(
      alert, granularity, "inner"
    ).filter(col("date").between(col("date_7_days_ahead"), col("alert_date"))
    ).select(selectedColumns.map(col): _*
    ).orderBy(col("date").desc
    )

    joinedWithDateRange.select(granularity.map(col) :+ col("date") :+ col("clicks"): _*)

  }

  def generateAlartDf(alertWithHistTraffic: DataFrame, granularity: Seq[String], runDate: String): DataFrame = {

    // find the total clicks
    val filteredDf = alertWithHistTraffic.filter(col("date") =!= runDate)
    val groupedDf = filteredDf.groupBy(granularity.map(col): _*
    ).agg(sum("clicks").alias("total_clicks")
    )

    // oder by desc(total_clicks), desc(date)
    val windowSpec = Window.partitionBy(granularity.map(col): _*).orderBy(col("total_clicks").desc, col("date").desc)
    alertWithHistTraffic.join(groupedDf, granularity
    ).withColumn("rank", row_number().over(windowSpec)
    ).orderBy(col("total_clicks").desc, col("date").desc
    ).drop("rank").drop("total_clicks")

  }

  def generateAlartDf2(alertWithHistTraffic: DataFrame, granularity: Seq[String], runDate: String): DataFrame = {

    val groupCols = granularity :+ "run_date"

    alertWithHistTraffic.orderBy(desc("date")).withColumn("run_date", lit(runDate)
    ).groupBy(groupCols.head, groupCols.tail: _*
    ).agg(
      collect_list("clicks").as("clicksList_day0_to_day7_before"),
      sum("clicks").alias("total_clicks")
    ).withColumn("clicksList_day0_to_day7_before",
      concat_ws(", ", col("clicksList_day0_to_day7_before"))
    ).orderBy(desc("total_clicks")
    ).drop("total_clicks"
    )

  }

  def sendDailyAlertEmail(spark: SparkSession, subject: String, toEmail: List[String], allAlertDir: String): Unit = {

    // Function to add missing columns with null values
    def addMissingColumns(df: DataFrame, allCols: Seq[String]): DataFrame =
      allCols.foldLeft(df) { (acc, colName) =>
        if (acc.columns.contains(colName)) acc else acc.withColumn(colName, lit(null))
      }

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val path = new Path(allAlertDir)

    // Exit if the alert directory doesn't exist or is empty
    if (!fs.exists(path) || fs.globStatus(path).isEmpty) {
      println("---- No alert ----")
      return
    }

    // Collect all unique columns from CSV files
    val allAlertPath =  s"$allAlertDir/*/p*"
    val allColumns = spark.sparkContext.wholeTextFiles(allAlertPath).map(_._1).collect().map(
      filePath => spark.read.option("header", "true").csv(filePath)
    ).foldLeft(Set.empty[String])((cols, df) => cols ++ df.columns
    ).toSeq

    // Standardize DataFrames and union them
    val standardizedDataFrames = spark.sparkContext.wholeTextFiles(allAlertPath).map(_._1).collect().map(
      filePath => addMissingColumns(spark.read.option("header", "true").csv(filePath), allColumns)
    )
    val unionedDataFrame = standardizedDataFrames.reduce(_ unionByName _)

    // Reorder and sort columns
    val columnOrder = Seq("run_date") ++ unionedDataFrame.columns.filterNot(col => col == "run_date" || col == "clicksList_day0_to_day7_before") ++ Seq("clicksList_day0_to_day7_before")
    val sortedDf = unionedDataFrame.select(columnOrder.head, columnOrder.tail: _*).orderBy(columnOrder.drop(1).dropRight(1).map(col): _*)

    // Prepare and send email
    val numRow = sortedDf.count().toInt
    val dfString = SparkUtils.showString(sortedDf, numRow, false)
//   val body = s"Below is a list of daily traffic that reduced dramatically based on historical trends (21 days):\n $dfString"

    val body = s"""
          |
          |Hi Team,
          |
          |The daily traffic alert is based on data from the eCommerceLayer: $ecommerceLayerDir, applied following thresholds on each granularity.
          |
          |Thresholds:
          |- Minimum Clicks: $minClicks
          |- Minimum IPUA: $minIPUA
          |- Minimum Domains: $minDomain
          |- Ratio Threshold: $ratioThreshold
          |
          |Granularities:
          |- Country
          |- Country, Website Name
          |- Country, Website Name, Action
          |- Country, Website Name, Action, Zone
          |
          |Here is the list of daily traffic that decreased significantly over the past 21 days, and the result is also on HDFS $allAlertDir.
          |
          |$dfString
          |
          |Best,
          |Data Platform Engineer Team
          |""".stripMargin

    println("---- send daily alert emails ----")
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

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val runDate = args(0)
    val outputPath = args(1)
    val emailList = args(2).split(",").map(x => x.trim).toList

//    val runDate = "2024-02-14"
//    val outputPath = "/user/qye/adhoc_detect_system_outage/QA_traffic_alert"

    println(s"----$runDate get ecommerce data for the past 21 days ---- ")
    val (ecomm21days, allDatesDF) = getRawDataPrev21Days(spark, runDate)

//      println("---- check each granularity ----")
    val granularityInputs = Seq(
      "country",
      "country, website_name",
      "country, website_name, action",
      "country, website_name, action, zone"
    )

    for (granularityInput <- granularityInputs) {

      // val granularityInput = "country"

      // granularity
      println("-------- " + granularityInput + "--------")
      val granularity = granularityInput.split(",").map(_.trim).toSeq
      val dailyGranularity = granularity :+ "date"

      // date string in paths
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val runDateObj = LocalDate.parse(runDate, formatter)
      val outputDateFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd")
      val outputDate = runDateObj.format(outputDateFormatter)
      val outputMonthFormatter = DateTimeFormatter.ofPattern("yyyy/MM")
      val outputMonth = runDateObj.format(outputMonthFormatter)

      // paths
      val granularityName = granularity.mkString("_")
      val alertPath = s"$outputPath/alert/$outputDate/$granularityName"
      val usagePath = s"$outputPath/usage/$outputMonth/$granularityName/p*"

//        println(usagePath)

      // get all combinations of granularity
      val values = ecomm21days.select(granularity.head, granularity.tail: _*).distinct()
      val combinationDateDF = values.crossJoin(allDatesDF)

//        println("-------- get ecommerceLayer data --------")
      val ecomm21daysFull = combinationDateDF.join(
        ecomm21days, dailyGranularity, "left"
      ).na.fill(0, Seq("clicks")
      )

//        println("-------- get daily clicks --------")
      val dailyRanks = getDailyClicks(ecomm21daysFull, granularity, runDate)

//        println("-------- get monthly usage reference ---------")
      val countDf = spark.read.format("parquet").load(usagePath)
      val dailyRankFull = dailyRanks.join(countDf, granularity, "left")

//        println("-------- get alert traffic --------")
      val alertWithHistTraffic = alertTraffic(dailyRankFull, granularity, runDate)
      val emailDf = generateAlartDf2(alertWithHistTraffic, granularity, runDate)

      if (emailDf.count() > 0) {
        saveFiles(emailDf, alertPath, "csv")
      }

    }

    val runDateOutput = runDate.replace("-", "_")
    val subject = s"($runDate) eCommerceLayer Daily Traffic Alert - Significant Drops"
    val allAlertDir = s"$outputPath/alert/$runDateOutput"

    sendDailyAlertEmail(spark, subject, emailList, allAlertDir)

  }
}
