package com.justinrmiller.sparkstreamingexample

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream

object Main {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: Main <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    args.foreach(println(_))

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("Main").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts: DStream[(String, Long)] = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.saveAsTextFiles("/tmp/omg")
    wordCounts.print

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
