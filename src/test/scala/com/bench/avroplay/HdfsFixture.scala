package com.bench.avroplay

import org.scalacheck.Gen
import tryllerylle._

trait HdfsFixture {
  val genFileName: Gen[String] = Gen.identifier
  val genChunk: Gen[Array[Byte]] = Gen.identifier.map(_.getBytes)
  val genChunks: Gen[List[Array[Byte]]] = Gen.listOfN(10, genChunk)

  val genBenchrows = for {
    str1 <- Gen.alphaStr
    str2 <- Gen.alphaUpperStr
  } yield benchrows(str1, str2)


}
