package com.bench.avroplay

import org.apache.avro._
import org.apache.avro.file.{DataFileStream, DataFileWriter, DataFileReader}
import org.apache.spark.{SparkConf, SparkContext} 
import tryllerylle._
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.specific.SpecificDatumReader
import tryllerylle.benchrows
import org.apache.spark.rdd.RDD
import org.apache.spark.input.PortableDataStream
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.avro.specific.SpecificDatumWriter
import java.io.File
import org.apache.avro.file.DataFileReader
import org.apache.hadoop.fs.FsServerDefaults
import org.apache.avro.file.SeekableFileInput
import java.{util => ju}
import org.apache.spark.sql.SparkSession

object Main extends App {
    val (inputFile, outputFile) = (args(0), args(1))
    Runner.run(new SparkConf(), inputFile, outputFile)
}

case class valueContainer(path: String)

object AvroCompactor{

    /*
        Fix writer, ensure also correct compression snappy is used
        TODO: Should we silently fail with empty vector, blow it up or log!?!?
    */
    def compactAvroFiles[T <: SpecificRecordBase](fileAvro: (String, Vector[(String, PortableDataStream)]))(implicit conf: Configuration, t: T) : Unit = {
        Try{
            val fs = FileSystem.get(conf)
            val path  = new Path( fileAvro._1)
            val defaults = fs.getServerDefaults(path)
            val out = fs.create(path, true, defaults.getFileBufferSize(), defaults.getReplication(), defaults.getBlockSize())
            val datumWriter = new SpecificDatumWriter[T]()
            val dataFileWriter = new DataFileWriter[T](datumWriter) 
            val datumReader = new SpecificDatumReader[T](t.getSchema())
            val outputWriter = dataFileWriter.create(t.getSchema(), out)
            var reuse : T = t

            fileAvro._2.foreach{
                case (filename, stream) =>
                    val input = stream.open()
                    val reader = new DataFileStream[T](input, datumReader)
                    while(reader.hasNext()){
                        reuse = reader.next(reuse)
                        outputWriter.append(reuse)
                    }
                    input.close()
            }
            outputWriter.close()
        } match {
            case Failure(exception) => throw exception // TODO : maybe some bookmarking for error handling like file names etc.
            case Success(value) => println("compacted files")
        }
    }
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
                
                // val transformation = dataFileReader
                //     .iterator()
                //     .asScala
                //     .map(r => r.copy(secondfield =  "i made it") )
                //     .filter(r => r.firstfield == "nogo")
                
                inputStream.close() //is this necesarry 
        }
    }
}
