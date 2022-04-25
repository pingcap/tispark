package com.pingcap.tispark.telemetry

import org.apache.spark.sql.test.SharedSQLContext
import org.json4s.jackson.JsonMethods.{compact, render}
import org.scalatest.FunSuite
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.{SharedSparkContext, SparkFunSuite}

class TeleMsgSuite extends SharedSQLContext{

  test ("xx") {
//    val teleMsg = new TeleMsg
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val objectWriter = mapper.writerWithDefaultPrettyPrinter

    case class Person(name: String, age: Int)
    case class Group(name: String, persons: Seq[Person], leader: Person)


    val jeroen = Person("Jeroen", 26)
    val martin = Person("Martin", 54)


    val originalGroup = Group("Scala ppl", Seq(jeroen,martin), martin)

    val originalMap = Map("a" -> List(1,2), "b" -> List(3,4,5), "c" -> List())
    println(objectWriter.writeValueAsString(TeleMsg))

    println("xx")
  }

}
