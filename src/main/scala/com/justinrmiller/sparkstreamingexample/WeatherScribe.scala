package com.justinrmiller.sparkstreamingexample

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.streaming._

/**
  * Created by Justin Miller on 6/21/17.
  *
  * Example JSON:
      { "version": 1, "station_name": "bobs_station", "timestamp": 1498114976232, "data": {"temperature": 12.0} }
  */
class WeatherScribe[K](sparkConf: SparkConf, kafkaParams: Map[String, Object])
  extends BaseScribe[K](sparkConf, kafkaParams) {
  override def batchDuration: Duration = Seconds(30)

  override def topics: Set[String] = Set("test")

  override def schema: StructType = {
    val data = (new StructType)
      .add("temperature", DoubleType)

    val root = (new StructType)
      .add("version", LongType)
      .add("station_name", StringType)
      .add("timestamp", LongType)
      .add("data", data)

    root
  }

  override def partitionFields: List[String] = List("version")

  override def modifyDataframe(inputDF: DataFrame): DataFrame = {
    inputDF.show()
    inputDF.printSchema()

    inputDF
  }
}
