package com.justinrmiller.sparkstreamingexample

import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.{SparkConf, TaskContext}

object Main {
  def generateKafkaParams(brokers: String) = {
    // Create direct kafka stream with brokers and topics]
    val kafkaParams = Map(
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    kafkaParams
  }

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

    val topicsSet = topics.split(",").toSet

    val batchDuration = Seconds(30)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("Main").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, batchDuration)

    val kafkaParams = generateKafkaParams(brokers)

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    messages.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }

      rdd.foreach(record => println(record.value))

      messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
