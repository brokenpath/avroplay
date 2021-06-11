package com.bench.avroplay

import org.scalacheck.Gen

trait HdfsFixture {
  val genFileName: Gen[String] = Gen.identifier
  val genChunk: Gen[Array[Byte]] = Gen.identifier.map(_.getBytes)
  val genChunks: Gen[List[Array[Byte]]] = Gen.listOfN(10, genChunk)
}
