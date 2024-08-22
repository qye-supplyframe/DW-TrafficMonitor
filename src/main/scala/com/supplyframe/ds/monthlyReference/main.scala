package com.supplyframe.ds.monthlyReference

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.time.YearMonth
import java.time.format.DateTimeFormatter

object main {

  def saveFiles(df: DataFrame, fn: String, TYPE: String): Unit = {
    if (TYPE == "avro") {
      df.coalesce(1).write.format("avro").mode("overwrite").save(fn)
    } else if (TYPE == "csv") {
      df.coalesce(1).write.mode("overwrite").option("header", "true").csv(fn + "_csv")
    } else if (TYPE == "parquet") {
      df.coalesce(1).write.format("parquet").mode("overwrite").save(fn)
    } else {
      df.write.format("avro").mode("overwrite").save(fn)
      df.coalesce(1).write.mode("overwrite").option("header", "true").csv(fn + "_csv")
    }
  }

  val ecommerceLayerDir = "/prod/etl/OrthogonalAggregations/EcommerceLayer"

  def getPathsForPrevYear(baseDir: String, runMonth: String): Seq[String] = {

//    val runMonth = "2024-07"
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM")
    val runMonthObj = YearMonth.parse(runMonth, formatter)
    val startMonth = runMonthObj.minusYears(1).format(formatter)
    val startMonthObj = YearMonth.parse(startMonth, formatter)

    // Generate folder paths
//    val monthsPath = generateMonthsDir(baseDir, startMonthObj, runMonthObj)
    val monthsPath = Iterator.iterate(startMonthObj)(_.plusMonths(1)
    ).takeWhile(month => month.isBefore(runMonthObj) // Stop before the end month
    ).map(month => s"${baseDir}/${month.getYear}/${f"${month.getMonthValue}%02d"}" // Generate folder paths
    ).toSeq

//    monthsPath.foreach(println)

    monthsPath

  }

  def generateUsage(rawTraffic: DataFrame, granularity: Seq[String]): DataFrame = {

    val dailyGranularity = granularity :+ "date"
    val dailyUsageRef = rawTraffic.groupBy(dailyGranularity.head, dailyGranularity.tail: _*).agg(
      countDistinct(col("master_user_group_id")).alias("count_master_user_group_id"),
      countDistinct(col("ip_address_user_agent")).alias("count_ip_address_user_agent"),
      countDistinct(col("city_code")).alias("count_city_code"),
      countDistinct(col("company_name")).alias("count_company_name"),
      countDistinct(col("domain_name")).alias("count_domain_name")
    )

    val countDf = dailyUsageRef.groupBy(granularity.head, granularity.tail: _*).agg(
      percentile_approx(col("count_master_user_group_id"), lit(0.5), lit(10000)).alias("median_count_master_user_group_id"),
      percentile_approx(col("count_ip_address_user_agent"), lit(0.5), lit(10000)).alias("median_count_ip_address_user_agent"),
      percentile_approx(col("count_domain_name"), lit(0.5), lit(10000)).alias("median_count_domain_name"),
      percentile_approx(col("count_company_name"), lit(0.5), lit(10000)).alias("median_count_company_name"),
      percentile_approx(col("count_city_code"), lit(0.5), lit(10000)).alias("median_count_city_code")
    )

    countDf

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

    val runMonth = args(0)
    val outputPath = args(1)

//    val runMonth = "2024-07"
//    val outputPath = "/user/qye/adhoc_detect_system_outage/QA_traffic_alert"
    println(runMonth)

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM")
    val runMonthObj = YearMonth.parse(runMonth, formatter)
    val outputMonthFormatter = DateTimeFormatter.ofPattern("yyyy/MM")
    val outputMonth = runMonthObj.format(outputMonthFormatter)

    // get usage reference over the past 12 months
    println("get past 12 months data")
    val eCommerce1yearPaths = getPathsForPrevYear(ecommerceLayerDir, runMonth)

    val ecomm1year = spark.read.parquet(eCommerce1yearPaths: _*)

    val granularityInputs = Seq("country",
      "country, website_name",
      "country, website_name, action",
      "country, website_name, action, zone"
    )

    for (granularityInput <- granularityInputs) {

      println(granularityInput)
      val granularity = granularityInput.split(",").map(_.trim).toSeq

      val countDf = generateUsage(ecomm1year.filter(col("clicks") === 1), granularity)

      val granularityName = granularity.mkString("_")
      val usagePath = s"$outputPath/usage/$outputMonth/$granularityName"
      saveFiles(countDf, usagePath, "parquet")

    }



//    val eCommerce1yearPaths = getPaths1YearBefore("/prod/etl/OrthogonalAggregations/EcommerceLayer", runDate)
//    val ecomm1year0 = eCommerce1yearPaths.map { path =>
//      spark.read.parquet(path)
//    }.reduce(_.union(_)).filter(col("country") =!= "***"
//    ).filter(col("date") >= oneYearBeforeStr
//    ).filter(col("date") <= runDate)
//
//    // iterate each granularity
//    val granularityInputs = Seq("country", "country, website_name")
//    var maxSave = Int.MinValue
////
//    for (granularityInput <- granularityInputs) {
//
//      println(granularityInput)
//      val granularity = granularityInput.split(",").map(_.trim).toSeq
//      val dailyGranularity = granularity :+ "date"
//
//      // Create a DataFrame with all possible combinations of granularity-values and dates
//      val allDates = generateDateRange(oneYearBeforeStr, runDate).map(date => Row(date))
//      val schema = StructType(StructField("date", StringType, nullable = true) :: Nil)
//      val allDatesDF = spark.createDataFrame(spark.sparkContext.parallelize(allDates), schema)
//
//      val values = ecomm1year0.select(granularity.head, granularity.tail: _*).distinct()
//      val combinationDateDF = values.crossJoin(allDatesDF)
//
//      val ecomm1year = combinationDateDF.join(
//        ecomm1year0, dailyGranularity, "left"
//      ).na.fill(0, Seq("clicks"))
//
//      ecomm1year.na.drop().show(false)
//
//      val currentSave = detectAlarmingTraffic(ecomm1year, runDate, granularity, datePath)
//
//      if (currentSave > maxSave) {
//        maxSave = currentSave
//      }
//    }
//
//    if (maxSave > 0) {
//      saveFiles(ecomm1year0, datePath, "parquet")
//    }


//
////    val res = spark.read.format("parquet").load(granularityPath + "/daily_rank")
//
//
//    alert.select(
//      "country", "website_name", "action",
//      "date", "clicks",  "daily_rank", "estimate1_avg_prev_7n14n21days",
//      "long_term_mean_user_count", "long_term_mean_domain_name_count", "ratio"
//    ).orderBy(desc("estimate1_avg_prev_7n14n21days"), desc("ratio")
//    ).show(40,false)
//
//    val selCols = dailyGranularity ++ Seq("clicks", "daily_rank", "ratio", "estimate")
//    val probDailyClicks = dailyClicks.join(
//      alert.select(dailyGranularity.head, dailyGranularity.tail: _*), dailyGranularity, "inner"
//    ).select(selCols.head, selCols.tail: _*
//    ).orderBy(desc("estimate"), desc("ratio")
//    )
//
//    saveFiles(probDailyClicks, granularityPath + "/alert", "csv")

  }
}
