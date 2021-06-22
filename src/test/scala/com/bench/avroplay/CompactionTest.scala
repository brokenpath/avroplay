package com.bench.avroplay

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FunSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.log4j.{Logger, Level}
import tryllerylle._

class CompactionTest extends FunSpec with BeforeAndAfterAll with BeforeAndAfterEach  with SparkSessionTestWrapper
{

    private val SMALL_BLOCKSIZE = 1024l
    private var miniHdfs: MiniDFSCluster = _
    private val dir = "./temp/hadoop"
    private val port: Int = 54310
    private val conf = new Configuration()
    conf.set("fs.default.name", s"hdfs://localhost:$port")
    conf.setBoolean("dfs.support.append", true)
    conf.set(
    "dfs.client.block.write.replace-datanode-on-failure.policy",
    "NEVER"
    ) //needed for performing append operation on hadoop-minicluster
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
                val filename = genFileName.sample.get
                val record = new tryllerylle.benchrows(genFileName.sample.get, genFileName.sample.get)
                AvroCompactor.writeRecords(List(record), filename, fs)

                val records = AvroCompactor.readRecords[benchrows](record.getSchema(), filename, fs)
                val diskRecord = records.next()
                assert(record == diskRecord)
            }
        }
        it("compact a few files, and ensure schema is the same"){

        }
        it("compact some files into a smaller number of files, and ensure same content"){
            new HdfsFixture {
                val filename_prefix = genFileName.sample.get
                val filenames = (1 to 20).map( i => filename_prefix + i.toString())
                val record = new tryllerylle.benchrows("012345678", "9ABCDEFG")
                var records = (1 to 20).toList.map(i => record)
                filenames.foreach(filename => AvroCompactor.writeRecords(records, filename, fs))
                val homeDir = fs.getHomeDirectory()
                val files = spark.sparkContext.binaryFiles(homeDir.toString())
                println("ITS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% HAPPENING")
                files.foreach{
                    case (filename, stream) => println(filename)
                }
                println("ITTTTTT ended ...")
            }
        }
    }


    override protected def beforeAll(): Unit = {
        val baseDir: File = new File(dir, "test")
        val miniDfsConf: HdfsConfiguration = new HdfsConfiguration

        miniDfsConf.set("dfs.namenode.fs-limits.min-block-size", "1024")
        miniDfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
        miniHdfs = new MiniDFSCluster.Builder(miniDfsConf)
            .nameNodePort(port)
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
