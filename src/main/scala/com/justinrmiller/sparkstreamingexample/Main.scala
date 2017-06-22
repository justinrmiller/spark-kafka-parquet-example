package com.justinrmiller.sparkstreamingexample

import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkConf

object Main {
  def generateKafkaParams(brokers: String): Map[String, Object] = {
    // Create direct kafka stream with brokers and topics]
    val kafkaParams = Map(
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],        // make this configurable
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    kafkaParams
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: Main <brokers>
        |  <brokers> is a list of one or more Kafka brokers
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    args.foreach(println(_))

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("Main").setMaster("local[4]")

    val kafkaParams = generateKafkaParams(brokers)

    val scribe = new WeatherScribe(sparkConf, kafkaParams)

    scribe.process("/tmp/data_in_parquet")

    // Start the computation
    scribe.streamingContext.start()
    scribe.streamingContext.awaitTermination()
  }
}
