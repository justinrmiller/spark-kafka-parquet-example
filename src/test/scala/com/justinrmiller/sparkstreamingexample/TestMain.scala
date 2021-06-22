package com.justinrmiller.sparkstreamingexample

import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.funsuite.AnyFunSuite

class TestMain extends AnyFunSuite {

  test("Main should generate the correct kafka params") {
    val actualOutput = Main.generateKafkaParams("localhost:9092")
    assert(actualOutput.keys.nonEmpty)

    assert(actualOutput == Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    ))
  }

}
