package com.bench.avroplay

import org.apache.avro._
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.apache.spark.{SparkConf, SparkContext} 
import tryllerylle._
import org.apache.avro.specific.SpecificRecordBase
import collection.JavaConverters._
import org.apache.avro.specific.SpecificDatumReader
import tryllerylle.benchrows
import org.apache.spark.rdd.RDD
import org.apache.spark.input.PortableDataStream

object Main extends App {
    val (inputFile, outputFile) = (args(0), args(1))
    Runner.run(new SparkConf(), inputFile, outputFile)
}


case class AvroFileGroups()

object Runner {
    val HADOOP_BLOCK_SIZE = 128 * 1024 * 1024  // improvement get dynamically from hdfs




    /*
        Map to vector and reduceByKey. 
        PortableDataStream is unopened we need to somehow do a foreach with a sideeffect open the file
        write loop though each datastream and close it.
        
    */
    def mergeAvro(rdd: RDD[(String, (String, PortableDataStream))]) : Unit = {
        
        
    }

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
                

                
                inputStream.close() //is this necesarry 

        }
        
    }
}
