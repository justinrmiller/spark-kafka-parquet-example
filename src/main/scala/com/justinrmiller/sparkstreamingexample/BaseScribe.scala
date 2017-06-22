package com.justinrmiller.sparkstreamingexample

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

/**
  * Created by justin on 6/21/17.
  */
abstract class BaseScribe[K](sparkConf: SparkConf, kafkaParams: Map[String, Object]) {
  def batchDuration: Duration
  def topics: Set[String]
  def schema: StructType
  def partitionFields: List[String]
  def modifyDataframe(inputDF: DataFrame): DataFrame = inputDF

  val sparkSession: SparkSession =
    SparkSession.builder().config(sparkConf).getOrCreate()
  val streamingContext = new StreamingContext(sparkSession.sparkContext, batchDuration)
  val sqlContext: SQLContext = sparkSession.sqlContext

  val dstream: InputDStream[ConsumerRecord[K, String]] = KafkaUtils.createDirectStream[K, String](
    streamingContext,
    PreferConsistent,
    Subscribe[K, String](topics, kafkaParams)
  )

  val customCallback = new DisplayOffsetCommits

  def process(path: String) {
    dstream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }

      if (!rdd.isEmpty) {
        val df = sqlContext.read.schema(schema).json(rdd.map(x => x.value))

        val finalDF = modifyDataframe(df)

        //todo "SOON"
        //val bloomFilter = df.stat.bloomFilter(colName = "station_name", expectedNumItems=2000, fpp=0.01)
        //println("bloom filter positive: " + bloomFilter.mightContainString("bobs_station"))
        //println("bloom filter negative: " + bloomFilter.mightContainString("tom"))

        finalDF.write
          .mode(SaveMode.Append)
          .partitionBy(partitionFields:_*)
          .format("parquet")
          .save(path)
      }

      dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges, customCallback)
    }
  }
}

class DisplayOffsetCommits extends OffsetCommitCallback {
  override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
    println(offsets)
    println(exception)
  }
}