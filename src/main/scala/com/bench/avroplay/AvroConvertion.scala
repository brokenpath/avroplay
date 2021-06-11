package com.bench.avroplay

import org.apache.avro._
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.apache.spark.{SparkConf, SparkContext} 
import trylerylle._
import org.apache.avro.specific.SpecificRecordBase
import collection.JavaConverters._
import org.apache.avro.specific.SpecificDatumReader
import tryllerylle.benchrows

object Main extends App {
    val (inputFile, outputFile) = (args(0), args(1))
    Runner.run(new SparkConf(), inputFile, outputFile)
}


object Runner {
    val HADOOP_BLOCK_SIZE = 128 * 1024 * 1024  // improvement get dynamically from hdfs

    def run(conf: SparkConf, path: String, outputFolder: String): Unit = {
        val sc = new SparkContext(conf)
        val rdd = sc.binaryFiles(path)
        rdd.map {
            case (inputFilepath, stream) =>
                val filename = inputFilepath.split("/").last
                val inputStream = stream.open()
                val datumReader = new SpecificDatumReader[benchrows](benchrows.SCHEMA$);
                val dataFileReader = new DataFileStream[benchrows](inputStream, datumReader)
                
                val transformation = dataFileReader
                    .iterator()
                    .asScala
                    .map(r => r.copy(secondfield =  "i made it") )
                    .filter(r => r.firstfield == "nogo")
                

                try{

                }

                inputStream.close() //is this necesarry 

        }
        
    }
}
