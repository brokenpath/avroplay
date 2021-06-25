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

}

class CompactionTest extends FunSpec with BeforeAndAfterAll with BeforeAndAfterEach  with SparkSessionTestWrapper
{

    private val SMALL_BLOCKSIZE = 1024l
    private var miniHdfs: MiniDFSCluster = _
    private val dir = "./temp/hadoop"
    private val conf = TestHelpers.newConf() //needed for performing append operation on hadoop-minicluster
    val fs: FileSystem = FileSystem.get(conf)
    Logger.getLogger("org").setLevel(Level.OFF)

    describe("modifyingFiles") {
        it("create file add, data and see its there") {
            println("")
            println("*********************************")
            println("*********************************")
            println("*********************************")
                new HdfsFixture {
                    val path: Path = new Path(genFileName.sample.get)
                    val defaults = fs.getServerDefaults(path)
                    val out = fs.create(path, true, defaults.getFileBufferSize(), defaults.getReplication(), SMALL_BLOCKSIZE)
                    out.write(genChunk.sample.get)
                    out.close()
                    val homeDir = fs.getHomeDirectory()
                    val files = fs.listStatus(homeDir)
                    for ( file <- files) {
                        println("--------------------------------------------------")
                        println(file)
                        println("--------------------------------------------------")
                    }
                }
            println("###################################")
            println("###################################")
            println("###################################")
        }
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
                val homeDir = fs.getHomeDirectory()
                val badfilesDir = new Path(homeDir.toString()+ "/badfiles")
                fs.mkdirs(badfilesDir)
                val filename_prefix = genFileName.sample.get
                val file_count = 20
                val filenames = (1 to file_count).map( 
                    i => new Path(badfilesDir.toString + "/" + filename_prefix + i.toString())
                )
                val record = TestHelpers.fixed_record()
                val record_count = 20
                var records = (1 to record_count).toList.map(i => record)
                filenames.foreach(filename => AvroCompactor.writeRecords(records, filename, fs))
                val files = spark.sparkContext.binaryFiles(badfilesDir.toString())
                
                files.map(v => (TestHelpers.compactedFilename, Vector(v))).reduceByKey{
                    case (v1, v2) => v1 ++ v2
                }
                .foreach{
                    v => AvroCompactor.compactAvroFiles(v)(TestHelpers.newConf(), TestHelpers.fixed_record())
                }

                val avroFile = new Path(TestHelpers.compactedFilename)
                val writtenRecords = AvroCompactor.readRecords[benchrows](
                    TestHelpers.fixed_record().getSchema(), 
                    avroFile, 
                    fs).toArray

                assert(writtenRecords.length == file_count * record_count)
                for (r <- writtenRecords) assert(r == TestHelpers.fixed_record())

            }
            it("a file doesnt exist ensure it fails with error message"){

            }
            it("reads a broken files ensure it fails with error message"){

            }
            it("compact a few files, and ensure schema is the same"){
                // Schema prop is not part of {hashcode, equals} so check the string repr :(

            }
            it("compact a few files of different 'evolution' and hope it doesnt break"){
                // We need to manually write schema and data or there will be a namespace clash
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
