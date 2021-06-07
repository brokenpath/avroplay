package com.bench.avroplay


import org.apache.spark.{SparkConf, SparkContext}



object Main extends App {
    val (inputFile, outputFile) = (args(0), args(1))
    Runner.run(new SparkConf(), inputFile, outputFile)
}


object Runner {
    def run(conf: SparkConf, inputFile: String, outputFile: String): Unit = {
        val sc = new SparkContext(conf)
        val rdd = sc.textFile(inputFile)
    }
}

object AvroConvertion  {
  
}
