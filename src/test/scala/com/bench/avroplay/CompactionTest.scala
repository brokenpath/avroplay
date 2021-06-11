package com.bench.avroplay

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FunSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}

class CompactionTest extends FunSpec with BeforeAndAfterAll with BeforeAndAfterEach with SparkSessionTestWrapper {

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

    describe("hasFiles") {
        it("create file add, data and see its there") {
            new HdfsFixture {
                val path: Path = new Path(genFileName.sample.get)
                val out = fs.create(path)
                out.write(genChunk.sample.get)
                out.close()
                val homeDir = fs.getHomeDirectory().getName()
                val f : File = new File("/user/" + homeDir)
                val files = f.listFiles()
                print(files)


            }


        }
    }


   override protected def beforeAll(): Unit = {
    val baseDir: File = new File(dir, "test")
    val miniDfsConf: HdfsConfiguration = new HdfsConfiguration
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
