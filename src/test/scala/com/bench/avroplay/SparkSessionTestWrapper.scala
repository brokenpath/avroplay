package com.bench.avroplay

import org.apache.spark.sql.SparkSession


trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local[4]").appName("spark session").getOrCreate()
  }

}
