package com.bench.avrotypes


import scala.annotation.switch

final case class WithSchemaProp(var firstfield: String, var secondfield: Option[String] = None, var thirdfield: Option[String]) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this("", None, None)
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        firstfield
      }.asInstanceOf[AnyRef]
      case 1 => {
        secondfield match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 2 => {
        thirdfield  match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.firstfield = {
        value.toString
      }.asInstanceOf[String]
      case 1 => this.secondfield = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 2 => this.thirdfield = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = WithSchemaProp.SCHEMA$
}

object WithSchemaProp {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"metainfo\":\"itsadog\",\"name\":\"WithSchemaProp\",\"namespace\":\"com.bench.avrotypes\",\"fields\":[{\"name\":\"firstfield\",\"type\":\"string\"},{\"name\":\"secondfield\",\"type\":[\"null\",\"string\"], \"default\": null},{\"name\":\"thirdfield\",\"type\":[\"null\",\"string\"], \"default\": null}]}") 
}