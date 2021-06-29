package com.bench.avroplay

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FunSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.log4j.{Logger, Level}
import tryllerylle._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.input.PortableDataStream
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericRecord
import com.bench.avrotypes.WithSchemaProp


object TestHelpers {
    def test( t: (String, PortableDataStream ), s : String ) = 
        (s, t) 

    val port = 54310

    def newConf() = {
        val conf = new Configuration()
        conf.set("fs.default.name", s"hdfs://localhost:$port")
        conf.setBoolean("dfs.support.append", true)
        conf.set(
        "dfs.client.block.write.replace-datanode-on-failure.policy",
        "NEVER"
        )
        conf
    }

    def fixed_record() = new tryllerylle.benchrows("012345678", "9ABCDEFG")


    val compactedFilename = "compacted.avro"

    val evolvedWithSchemaProp = new org.apache.avro.Schema.Parser().parse(
        """{"type":"record",
            "metainfo":"itsadog",
            "newfield":"bigfile",
            "name":"WithSchemaProp","namespace":"com.bench.avrotypes",
            "fields":[{"name":"firstfield","type":"string"},
            {"name":"secondfield","type":["null","string"], "default": null},
            {"name":"thirdfield","type":["null","string"], "default": null },
            {"name":"fourthfield","type":["null","string"], "default": null }
            ]}""")

    val devolvedWithSchemaProp = new org.apache.avro.Schema.Parser().parse(
        """{"type":"record",
            "metainfo":"itsadog",
            "newfield":"bigfile",
            "name":"WithSchemaProp","namespace":"com.bench.avrotypes",
            "fields":[{"name":"firstfield","type":"string"}
            ]}""")

}

class CompactionTest extends FunSpec with BeforeAndAfterAll with BeforeAndAfterEach  with SparkSessionTestWrapper
{

    private val SMALL_BLOCKSIZE = 1024l
    private var miniHdfs: MiniDFSCluster = _
    private val dir = "./temp/hadoop"
    private val conf = TestHelpers.newConf() //needed for performing append operation on hadoop-minicluster
    val fs: FileSystem = FileSystem.get(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    private val homeDir = fs.getHomeDirectory()

    describe("Basic test") {
        it("create a avro file, adds a record and reads it"){
            new HdfsFixture {
                val path =  new Path(genFileName.sample.get)
                val record = new tryllerylle.benchrows(genFileName.sample.get, genFileName.sample.get)
                AvroCompactor.writeRecords(List(record), path, fs)

                val records = AvroCompactor.readRecords[benchrows](record.getSchema(), path, fs)
                val diskRecord = records.next()
                assert(record == diskRecord)
            }
        }
        it("compact fixed number files into a a single file, and ensure same content"){
            new HdfsFixture {
                val badfilesDir = new Path(homeDir.toString()+ "/badfiles")
                fs.mkdirs(badfilesDir)
                val filename_prefix = genFileName.sample.get
                val file_count = 20
                val filenames = (1 to file_count).map( 
                    i => new Path(badfilesDir.toString + "/" + filename_prefix + i.toString())
                )
                val record = TestHelpers.fixed_record()
                val record_count = 20
                val records = (1 to record_count).toList.map(i => record)
                filenames.foreach(filename => AvroCompactor.writeRecords(records, filename, fs))
                val files = spark.sparkContext.binaryFiles(badfilesDir.toString())
                
                files.map(v => (TestHelpers.compactedFilename, Vector(v))).reduceByKey{
                    case (v1, v2) => v1 ++ v2
                }
                .foreach{
                    v => AvroCompactor.compactAvroFiles(v)(TestHelpers.newConf(), 
                        TestHelpers.fixed_record())
                }

                val avroFile = new Path(TestHelpers.compactedFilename)
                val writtenRecords = AvroCompactor.readRecords[benchrows](
                    TestHelpers.fixed_record().getSchema(), 
                    avroFile, 
                    fs).toArray

                assert(writtenRecords.length == file_count * record_count)
                for (r <- writtenRecords) assert(r == TestHelpers.fixed_record())

            }
        }
        it("a file doesnt exist ensure it fails with error message"){
            val idontexist = new Path(homeDir.toString() + "/magicalfileonthemoon")
            val files = spark.sparkContext.binaryFiles(idontexist.toString())
            assertThrows[org.apache.hadoop.mapreduce.lib.input.InvalidInputException]{
                files.map(v => (TestHelpers.compactedFilename, Vector(v))
                    ).foreach {
                    v => AvroCompactor.compactAvroFiles(v)(TestHelpers.newConf(), 
                            TestHelpers.fixed_record())
                }
            }
        }
        it("reads a broken files ensure it fails with error message"){
            new HdfsFixture {
                val brokenfile: Path = new Path(homeDir.toString() + "/brokenavro.avro")
                val out = fs.create(brokenfile)
                out.write(genChunk.sample.get)
                out.close()
                val files = spark.sparkContext.binaryFiles(brokenfile.toString())
                //Inner exception is hidden which is not so great SparkException seems to generic
                assertThrows[org.apache.spark.SparkException]{
                    files.map(v => (TestHelpers.compactedFilename, Vector(v))
                        ).foreach {
                        v => AvroCompactor.compactAvroFiles(v)(TestHelpers.newConf(), 
                                TestHelpers.fixed_record())
                        }
                }
            }

        }
        it("compact a few files of different 'evolution', and ensure schema is the same"){
            // Schema prop is not part of {hashcode, equals} so check the string repr :(
            new HdfsFixture {
                val data = genWithSchemaPropFiles.sample.get
                val headIsEmpty = data.map(  l => 
                    l.headOption.isEmpty.toString()).
                    reduce[String] {
                        case (b1,b2) => b1 + ", " + b2
                    }

                
                println("##########################")
                println(s"Is the head empty: $headIsEmpty")
                val diffschemadir = new Path(homeDir.toString()+ "/diffschemadir")
                fs.mkdirs(diffschemadir)
                val prefix = genFileName.sample.get
                val absolutePrefix = diffschemadir.toString() + "/" + prefix
                var i = 0
                def nexti() = { i = i + 1; i.toString()}
                data.foreach(records => AvroCompactor.writeRecords(records, new Path( absolutePrefix + nexti()), fs))

                ////// Devolved records
                val recordbuilder = new GenericRecordBuilder(TestHelpers.devolvedWithSchemaProp)
                val devolvedRecords = getListOfStr(1234).sample.get.map{
                    v1 =>
                     {
                        recordbuilder.set("firstfield", v1) 
                        recordbuilder.build()
                    }
                }

                val out = fs.create(new Path( absolutePrefix + nexti()), true)
                val writer = new GenericDatumWriter[GenericRecord](TestHelpers.devolvedWithSchemaProp)
                val dataFileWriter = new DataFileWriter(writer)
                val outputWriter = dataFileWriter.create(TestHelpers.devolvedWithSchemaProp, out)
                devolvedRecords.foreach( r => outputWriter.append(r))
                outputWriter.close()
                ////// Evolved records
                val recordbuilder2 = new GenericRecordBuilder(TestHelpers.evolvedWithSchemaProp)
                
                
                val evolvedRecords = genListOf4Tuple(324).sample.get.map{
                    case(v1,v2,v3,v4) =>
                     {
                        recordbuilder2.set("firstfield", v1)
                        recordbuilder2.set("secondfield", v2.getOrElse(null))
                        recordbuilder2.set("thirdfield", v3.getOrElse(null))
                        recordbuilder2.set("fourthfield", v4.getOrElse(null))
                        recordbuilder2.build()
                    }
                }

                val out2 = fs.create(new Path( absolutePrefix + nexti()), true)
                val writer2 = new GenericDatumWriter[GenericRecord](TestHelpers.evolvedWithSchemaProp)
                val dataFileWriter2 = new DataFileWriter(writer2)
                val outputWriter2 = dataFileWriter2.create(TestHelpers.evolvedWithSchemaProp, out2)
                evolvedRecords.foreach( r => outputWriter2.append(r))
                outputWriter2.close()

                // compaction and test
                val files = spark.sparkContext.binaryFiles(diffschemadir.toString())
                
                files.map(v => (TestHelpers.compactedFilename, Vector(v))).reduceByKey{
                    case (v1, v2) => v1 ++ v2
                }.foreach{
                    v => AvroCompactor.compactAvroFiles(v)(TestHelpers.newConf(), 
                        WithSchemaProp("test", None, None))
                }

                
            }

        }
    }


    override protected def beforeAll(): Unit = {
        val baseDir: File = new File(dir, "test")
        val miniDfsConf: HdfsConfiguration = new HdfsConfiguration

        miniDfsConf.set("dfs.namenode.fs-limits.min-block-size", "1024")
        miniDfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
        miniHdfs = new MiniDFSCluster.Builder(miniDfsConf)
            .nameNodePort(TestHelpers.port)
            .format(true)
            .build()
        miniHdfs.waitClusterUp()
    }

    override protected def afterAll(): Unit = {
        fs.close()
        miniHdfs.shutdown()
    }

    override protected def afterEach(): Unit = {
        fs.delete(new Path(dir), true)
    }
}
